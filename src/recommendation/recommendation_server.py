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
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from opentelemetry import trace, metrics
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

# Fallback product IDs for when product-catalog is unavailable
FALLBACK_PRODUCT_IDS = ["OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM", "2ZYFJ3GM2N", "0PUK6V6EV0", "LS4PSXUNUM", "9SIQT8TOJO"]

# gRPC timeout configuration (seconds)
GRPC_TIMEOUT = 5.0

class RecommendationService(demo_pb2_grpc.RecommendationServiceServicer):
    def ListRecommendations(self, request, context):
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

        # Feature flag scenario - Cache Leak
        if check_feature_flag("recommendationCacheFailure"):
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                
                # Use safe product catalog call with error handling
                response_ids, catalog_success = get_product_list_from_catalog()
                span.set_attribute("app.catalog.success", catalog_success)
                
                if catalog_success:
                    cached_ids = cached_ids + response_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                else:
                    # Use fallback when catalog fails, but maintain cache behavior
                    if not cached_ids:
                        cached_ids = response_ids  # Use fallback as initial cache
                    product_ids = cached_ids
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                # If cache is empty due to previous failures, populate with fallback
                if not cached_ids:
                    cached_ids = FALLBACK_PRODUCT_IDS
                product_ids = cached_ids
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            # Use safe product catalog call with error handling
            product_ids, catalog_success = get_product_list_from_catalog()
            span.set_attribute("app.catalog.success", catalog_success)

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        
        # Ensure we have products to recommend
        if num_products == 0:
            logger.warning("No products available after filtering, using fallback recommendations")
            return get_fallback_recommendations(request_product_ids, max_responses)
        
        num_return = min(max_responses, num_products)

        # Sample list of indices to return
        if num_return > 0:
            indices = random.sample(range(num_products), num_return)
            # Fetch product ids from indices
            prod_list = [filtered_products[i] for i in indices]
        else:
            prod_list = []

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


def get_fallback_recommendations(request_product_ids=None, max_responses=5):
    """Return fallback product recommendations when product-catalog is unavailable."""
    if request_product_ids is None:
        request_product_ids = []
    
    # Filter out requested products from fallback list
    filtered_products = list(set(FALLBACK_PRODUCT_IDS) - set(request_product_ids))
    num_return = min(max_responses, len(filtered_products))
    
    if num_return == 0:
        return FALLBACK_PRODUCT_IDS[:max_responses]
    
    # Return random sample of fallback products
    return random.sample(filtered_products, num_return)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((grpc.RpcError,))
)
def call_product_catalog_with_retry():
    """Call product catalog with retry logic for transient failures."""
    return product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=GRPC_TIMEOUT)


def get_product_list_from_catalog():
    """Safely get product list from catalog with comprehensive error handling."""
    try:
        cat_response = call_product_catalog_with_retry()
        product_ids = [x.id for x in cat_response.products]
        logger.info(f"Successfully retrieved {len(product_ids)} products from catalog")
        return product_ids, True
    except grpc.RpcError as e:
        logger.error(f"gRPC call to product-catalog failed: {e.code()}, {e.details()}")
        if e.code() == StatusCode.UNAVAILABLE:
            logger.warning("Product-catalog service unavailable, using fallback recommendations")
        elif e.code() == StatusCode.DEADLINE_EXCEEDED:
            logger.warning("Product-catalog service timeout, using fallback recommendations")
        else:
            logger.error(f"Unexpected gRPC error: {e.code()}")
        return FALLBACK_PRODUCT_IDS, False
    except Exception as e:
        logger.error(f"Unexpected error calling product-catalog: {str(e)}")
        return FALLBACK_PRODUCT_IDS, False


def validate_product_catalog_connection(stub, max_retries=3, retry_delay=2):
    """Validate connection to product catalog during startup."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Validating product-catalog connection (attempt {attempt + 1}/{max_retries})")
            # Test connection with a simple call
            response = stub.ListProducts(demo_pb2.Empty(), timeout=GRPC_TIMEOUT)
            logger.info(f"Product-catalog connection validated successfully. Found {len(response.products)} products.")
            return True
        except grpc.RpcError as e:
            logger.warning(f"Product-catalog connection validation failed: {e.code()}, {e.details()}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Product-catalog connection validation failed after all retries. Service will use fallback recommendations.")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during product-catalog connection validation: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return False
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
        ('grpc.max_connection_idle_ms', 60000),
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    # Validate connection to product catalog during startup
    catalog_connected = validate_product_catalog_connection(product_catalog_stub)
    if not catalog_connected:
        logger.warning("Starting recommendation service with fallback mode due to product-catalog connectivity issues")

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
