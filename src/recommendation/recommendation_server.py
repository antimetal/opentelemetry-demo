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

# Default fallback product recommendations
FALLBACK_PRODUCT_IDS = ["OLJCESPC7Z", "66VCHSJNUP", "1YMWWN1N4O", "L9ECAV7KIM", "2ZYFJ3GM2N"]

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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(grpc.RpcError)
)
def call_product_catalog_with_retry():
    """Make gRPC call to product catalog with retry logic for transient failures"""
    return product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=5.0)


def get_fallback_recommendations(request_product_ids, max_responses=5):
    """Return fallback recommendations when product catalog is unavailable"""
    # Filter out requested products from fallback list
    filtered_fallback = list(set(FALLBACK_PRODUCT_IDS) - set(request_product_ids))
    num_return = min(max_responses, len(filtered_fallback))
    if num_return > 0:
        return random.sample(filtered_fallback, num_return)
    return FALLBACK_PRODUCT_IDS[:max_responses]


def get_product_list(request_product_ids):
    global first_run
    global cached_ids
    with tracer.start_as_current_span("get_product_list") as span:
        max_responses = 5

        # Formulate the list of characters to list of strings
        request_product_ids_str = ''.join(request_product_ids)
        request_product_ids = request_product_ids_str.split(',')

        try:
            # Feature flag scenario - Cache Leak
            if check_feature_flag("recommendationCacheFailure"):
                span.set_attribute("app.recommendation.cache_enabled", True)
                if random.random() < 0.5 or first_run:
                    first_run = False
                    span.set_attribute("app.cache_hit", False)
                    logger.info("get_product_list: cache miss")
                    
                    try:
                        cat_response = call_product_catalog_with_retry()
                        response_ids = [x.id for x in cat_response.products]
                        cached_ids = cached_ids + response_ids
                        cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                        product_ids = cached_ids
                        span.set_attribute("app.product_catalog.success", True)
                    except grpc.RpcError as e:
                        logger.error(f"Product catalog gRPC call failed: {e.code()}, {e.details()}")
                        span.set_attribute("app.product_catalog.success", False)
                        span.set_attribute("app.product_catalog.error", str(e.code()))
                        # Use fallback recommendations on failure
                        return get_fallback_recommendations(request_product_ids, max_responses)
                    except Exception as e:
                        logger.error(f"Unexpected error calling product catalog: {str(e)}")
                        span.set_attribute("app.product_catalog.success", False)
                        span.set_attribute("app.product_catalog.error", "unexpected_error")
                        return get_fallback_recommendations(request_product_ids, max_responses)
                else:
                    span.set_attribute("app.cache_hit", True)
                    logger.info("get_product_list: cache hit")
                    # If cache is empty (previous failures), use fallback
                    if not cached_ids:
                        logger.warning("Cache is empty, using fallback recommendations")
                        return get_fallback_recommendations(request_product_ids, max_responses)
                    product_ids = cached_ids
            else:
                span.set_attribute("app.recommendation.cache_enabled", False)
                try:
                    cat_response = call_product_catalog_with_retry()
                    product_ids = [x.id for x in cat_response.products]
                    span.set_attribute("app.product_catalog.success", True)
                except grpc.RpcError as e:
                    logger.error(f"Product catalog gRPC call failed: {e.code()}, {e.details()}")
                    span.set_attribute("app.product_catalog.success", False)
                    span.set_attribute("app.product_catalog.error", str(e.code()))
                    # Use fallback recommendations on failure
                    return get_fallback_recommendations(request_product_ids, max_responses)
                except Exception as e:
                    logger.error(f"Unexpected error calling product catalog: {str(e)}")
                    span.set_attribute("app.product_catalog.success", False)
                    span.set_attribute("app.product_catalog.error", "unexpected_error")
                    return get_fallback_recommendations(request_product_ids, max_responses)

            span.set_attribute("app.products.count", len(product_ids))

            # Create a filtered list of products excluding the products received as input
            filtered_products = list(set(product_ids) - set(request_product_ids))
            num_products = len(filtered_products)
            span.set_attribute("app.filtered_products.count", num_products)
            
            # If no products available after filtering, use fallback
            if num_products == 0:
                logger.warning("No products available after filtering, using fallback recommendations")
                return get_fallback_recommendations(request_product_ids, max_responses)
                
            num_return = min(max_responses, num_products)

            # Sample list of indicies to return
            indices = random.sample(range(num_products), num_return)
            # Fetch product ids from indices
            prod_list = [filtered_products[i] for i in indices]

            span.set_attribute("app.filtered_products.list", prod_list)
            return prod_list
            
        except Exception as e:
            # Catch-all for any unexpected errors to prevent service crashes
            logger.error(f"Unexpected error in get_product_list: {str(e)}")
            span.set_attribute("app.recommendation.fallback_used", True)
            span.set_attribute("app.recommendation.error", str(e))
            return get_fallback_recommendations(request_product_ids, max_responses)


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
        ('grpc.http2.min_ping_interval_without_data_ms', 300000),
        ('grpc.http2.max_pings_without_data', 0),
    ]
    
    pc_channel = grpc.insecure_channel(catalog_addr, options=options)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)
    
    # Test connection during startup with timeout
    logger.info(f"Testing connection to product catalog at {catalog_addr}")
    try:
        # Test with a short timeout to fail fast during startup if service is unavailable
        test_response = product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=2.0)
        logger.info(f"Successfully connected to product catalog, found {len(test_response.products)} products")
    except grpc.RpcError as e:
        logger.warning(f"Product catalog connection test failed: {e.code()}, {e.details()}. Service will use fallback recommendations.")
    except Exception as e:
        logger.warning(f"Unexpected error testing product catalog connection: {str(e)}. Service will use fallback recommendations.")

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
