#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
import time
from concurrent import futures

# Pip
import grpc
from grpc import StatusCode
from opentelemetry import trace, metrics
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import (
    OTLPLogExporter,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from openfeature import api
from openfeature.contrib.provider.flagd import FlagdProvider

from openfeature.contrib.hook.opentelemetry import TracingHook

# Local
import logging
import demo_pb2
import demo_pb2_grpc
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from metrics import (
    init_metrics
)

cached_ids = []
first_run = True

# Fallback product IDs for when product catalog is unavailable
FALLBACK_PRODUCT_IDS = ["OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM", "2ZYFJ3GM2N", "9SIQT8TOJO", "6E92ZMYYFZ", "0PUK6V6EV0", "LS4PSXUNUM", "2ZYFJ3GM2N"]

# Connection health tracking
product_catalog_available = True
last_failure_time = 0
FAILURE_THRESHOLD = 60  # seconds

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
        try:
            prod_list = get_product_list(request.product_ids)
            span = trace.get_current_span()
            span.set_attribute("app.products_recommended.count", len(prod_list))
            logger.info(f"Receive ListRecommendations for product ids:{prod_list}")

            # build and return response
            response = demo_pb2.ListRecommendationsResponse()
            response.product_ids.extend(prod_list)

            # Collect metrics for this service
            rec_svc_metrics["app_recommendations_counter"].add(len(prod_list), {'recommendation.type': 'catalog'})

            return response
        except Exception as e:
            logger.error(f"Error in ListRecommendations: {str(e)}")
            # Return fallback recommendations to prevent service failure
            fallback_list = get_fallback_recommendations(request.product_ids)
            response = demo_pb2.ListRecommendationsResponse()
            response.product_ids.extend(fallback_list)
            rec_svc_metrics["app_recommendations_counter"].add(len(fallback_list), {'recommendation.type': 'fallback'})
            return response

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING)

    def Watch(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.UNIMPLEMENTED)


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        # Check circuit breaker before attempting product catalog calls
        if not is_product_catalog_available():
            logger.warning("Product catalog circuit breaker is open, using fallback recommendations")
            span.set_attribute("app.recommendation.fallback_used", True)
            return get_fallback_recommendations(request_product_ids)

        try:
            # Feature flag scenario - Cache Leak
            if check_feature_flag("recommendationCacheFailure"):
                span.set_attribute("app.recommendation.cache_enabled", True)
                if random.random() < 0.5 or first_run:
                    first_run = False
                    span.set_attribute("app.cache_hit", False)
                    logger.info("get_product_list: cache miss")
                    cat_response = call_product_catalog_with_retry()
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    span.set_attribute("app.cache_hit", True)
                    logger.info("get_product_list: cache hit")
                    product_ids = cached_ids
            else:
                span.set_attribute("app.recommendation.cache_enabled", False)
                cat_response = call_product_catalog_with_retry()
                product_ids = [x.id for x in cat_response.products]

            # Mark successful call
            mark_product_catalog_success()
            span.set_attribute("app.products.count", len(product_ids))

        except Exception as e:
            logger.error(f"Failed to get products from catalog: {str(e)}")
            mark_product_catalog_failure()
            span.set_attribute("app.recommendation.catalog_failed", True)
            # Use fallback recommendations when catalog fails
            return get_fallback_recommendations(request_product_ids)

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

        if num_products == 0:
            # If no products after filtering, return fallback
            return get_fallback_recommendations(request_product_ids)

        # Sample list of indicies to return
        indices = random.sample(range(num_products), num_return)
        # Fetch product ids from indices
        prod_list = [filtered_products[i] for i in indices]

        span.set_attribute("app.filtered_products.list", prod_list)

        return prod_list


def must_map_env(key: str):
    value = os.environ.get(key)
    if value is None:
        raise Exception(f'{key} environment variable must be set')
    return value


def check_feature_flag(flag_name: str):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_boolean_value("recommendationCacheFailure", False)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type(grpc.RpcError)
)
def call_product_catalog_with_retry():
    """Call product catalog with retry logic and timeout."""
    try:
        # Add timeout to prevent indefinite blocking
        return product_catalog_stub.ListProducts(
            demo_pb2.Empty(),
            timeout=5.0  # 5 second timeout
        )
    except grpc.RpcError as e:
        logger.error(f"gRPC call failed: {e.code()}, {e.details()}")
        if e.code() == StatusCode.UNAVAILABLE:
            logger.warning("Product catalog service is unavailable")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in product catalog call: {str(e)}")
        raise


def get_fallback_recommendations(request_product_ids):
    """Return fallback recommendations when product catalog is unavailable."""
    # Convert request_product_ids to list if it's a string
    if isinstance(request_product_ids, str):
        request_product_ids = request_product_ids.split(',')
    
    # Filter out requested products from fallback list
    filtered_fallback = [pid for pid in FALLBACK_PRODUCT_IDS if pid not in request_product_ids]
    
    # Return up to 5 recommendations
    max_responses = 5
    num_return = min(max_responses, len(filtered_fallback))
    
    if num_return == 0:
        # If all fallback products were requested, return first 5 from fallback list
        return FALLBACK_PRODUCT_IDS[:max_responses]
    
    return filtered_fallback[:num_return]


def is_product_catalog_available():
    """Circuit breaker: check if product catalog should be considered available."""
    global product_catalog_available, last_failure_time
    
    if product_catalog_available:
        return True
    
    # Check if enough time has passed to retry
    if time.time() - last_failure_time > FAILURE_THRESHOLD:
        logger.info("Circuit breaker: attempting to reset, trying product catalog again")
        product_catalog_available = True
        return True
    
    return False


def mark_product_catalog_success():
    """Mark product catalog as healthy."""
    global product_catalog_available
    if not product_catalog_available:
        logger.info("Product catalog recovered, circuit breaker closed")
    product_catalog_available = True


def mark_product_catalog_failure():
    """Mark product catalog as unhealthy and open circuit breaker."""
    global product_catalog_available, last_failure_time
    product_catalog_available = False
    last_failure_time = time.time()
    logger.warning(f"Product catalog marked as unavailable, circuit breaker opened for {FAILURE_THRESHOLD} seconds")


def validate_product_catalog_connection():
    """Validate connection to product catalog during startup."""
    try:
        logger.info("Validating product catalog connection...")
        # Test connection with short timeout
        test_response = product_catalog_stub.ListProducts(
            demo_pb2.Empty(),
            timeout=2.0
        )
        logger.info(f"Product catalog connection validated successfully, found {len(test_response.products)} products")
        return True
    except grpc.RpcError as e:
        logger.warning(f"Product catalog connection validation failed: {e.code()}, {e.details()}")
        mark_product_catalog_failure()
        return False
    except Exception as e:
        logger.warning(f"Product catalog connection validation failed with unexpected error: {str(e)}")
        mark_product_catalog_failure()
        return False


if __name__ == "__main__":
    service_name = must_map_env('OTEL_SERVICE_NAME')
    api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
    api.add_hooks([TracingHook()])

    # Initialize Traces and Metrics
    tracer = trace.get_tracer_provider().get_tracer(service_name)
    meter = metrics.get_meter_provider().get_meter(service_name)
    rec_svc_metrics = init_metrics(meter)

    # Initialize Logs
    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                'service.name': service_name,
            }
        ),
    )
    set_logger_provider(logger_provider)
    log_exporter = OTLPLogExporter(insecure=True)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

    # Attach OTLP handler to logger
    logger = logging.getLogger('main')
    logger.addHandler(handler)

    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    
    # Configure gRPC channel with keepalive and timeout options
    options = [
        ('grpc.keepalive_time_ms', 30000),
        ('grpc.keepalive_timeout_ms', 10000),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.http2.min_ping_interval_without_data_ms', 300000),
        ('grpc.http2.max_ping_strikes', 5),
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    # Validate product catalog connection on startup
    validate_product_catalog_connection()

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # Start server
    port = must_map_env('RECOMMENDATION_PORT')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f'Recommendation service started, listening on port {port}')
    server.wait_for_termination()
