#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
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
FALLBACK_PRODUCT_IDS = [
    "OLJCESPC7Z",  # Vintage Typewriter
    "66VCHSJNUP",  # Vintage Camera Lens
    "1YMWWN1N4O",  # Home Barista Kit
    "L9ECAV7KIM",  # Loafers
    "2ZYFJ3GM2N",  # Vintage Record Player
    "0PUK6V6EV0",  # Vintage Bike
    "LS4PSXUNUM",  # Metal Camping Mug
    "9SIQT8TOJO",  # City Bike
    "6E92ZMYYFZ",  # Air Plant
    "HQTGWGPNH4"   # Salt & Pepper Shakers
]

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


def get_fallback_recommendations(request_product_ids, max_responses=5):
    """Return fallback recommendations when product-catalog is unavailable."""
    # Create a filtered list of products excluding the products received as input
    filtered_products = list(set(FALLBACK_PRODUCT_IDS) - set(request_product_ids))
    num_products = len(filtered_products)
    num_return = min(max_responses, num_products)
    
    if num_return == 0:
        return FALLBACK_PRODUCT_IDS[:max_responses]
    
    # Sample list of indices to return
    indices = random.sample(range(num_products), num_return)
    # Fetch product ids from indices
    return [filtered_products[i] for i in indices]


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(grpc.RpcError)
)
def call_product_catalog_with_retry():
    """Call product catalog with retry logic for transient failures."""
    return product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=5.0)


def get_product_list_from_catalog():
    """Get product list from catalog with comprehensive error handling."""
    try:
        cat_response = call_product_catalog_with_retry()
        return [x.id for x in cat_response.products]
    except grpc.RpcError as e:
        logger.error(f"gRPC call to product-catalog failed: {e.code()}, {e.details()}")
        if e.code() in [StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED, StatusCode.UNKNOWN]:
            logger.info("Using fallback recommendations due to product-catalog unavailability")
            return None  # Signal to use fallback
        # For other gRPC errors, re-raise to let caller handle
        raise
    except Exception as e:
        logger.error(f"Unexpected error calling product-catalog: {str(e)}")
        return None  # Signal to use fallback


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
                
                # Try to get products from catalog with error handling
                catalog_product_ids = get_product_list_from_catalog()
                if catalog_product_ids is not None:
                    cached_ids = cached_ids + catalog_product_ids
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    product_ids = cached_ids
                    span.set_attribute("app.product_source", "catalog")
                else:
                    # Fallback to default recommendations
                    product_ids = get_fallback_recommendations(request_product_ids, max_responses)
                    span.set_attribute("app.product_source", "fallback")
                    span.set_attribute("app.recommendation.degraded", True)
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids
                span.set_attribute("app.product_source", "cache")
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            
            # Try to get products from catalog with error handling
            catalog_product_ids = get_product_list_from_catalog()
            if catalog_product_ids is not None:
                product_ids = catalog_product_ids
                span.set_attribute("app.product_source", "catalog")
            else:
                # Fallback to default recommendations
                product_ids = get_fallback_recommendations(request_product_ids, max_responses)
                span.set_attribute("app.product_source", "fallback")
                span.set_attribute("app.recommendation.degraded", True)

        span.set_attribute("app.products.count", len(product_ids))

        # Create a filtered list of products excluding the products received as input
        filtered_products = list(set(product_ids) - set(request_product_ids))
        num_products = len(filtered_products)
        span.set_attribute("app.filtered_products.count", num_products)
        num_return = min(max_responses, num_products)

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
    
    # Configure gRPC channel with timeout and keepalive options
    options = [
        ('grpc.keepalive_time_ms', 30000),
        ('grpc.keepalive_timeout_ms', 10000),
        ('grpc.http2.max_pings_without_data', 0),
        ('grpc.http2.min_time_between_pings_ms', 10000),
        ('grpc.so_reuseport', 1),
        ('grpc.max_receive_message_length', 4 * 1024 * 1024),  # 4MB
        ('grpc.max_send_message_length', 4 * 1024 * 1024),     # 4MB
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    # Validate connection during startup
    try:
        # Test connectivity with a quick health check
        health_check_response = product_catalog_stub.ListProducts(
            demo_pb2.Empty(), 
            timeout=10.0
        )
        logger.info(f"Successfully connected to product-catalog at {catalog_addr}")
    except grpc.RpcError as e:
        logger.warning(f"Product-catalog connection test failed: {e.code()}, {e.details()}. Service will use fallback recommendations when needed.")
    except Exception as e:
        logger.warning(f"Unexpected error testing product-catalog connection: {str(e)}. Service will use fallback recommendations when needed.")

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
