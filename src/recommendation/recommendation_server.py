#!/usr/bin/python

# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


# Python
import os
import random
import sys
from concurrent import futures

# Pip
import grpc
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
        try:
            # Test product-catalog connectivity
            cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=5)
            logger.info("Health check passed: product-catalog is responsive")
            return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)
        except grpc.RpcError as e:
            logger.error(f"Health check failed: product-catalog unreachable - {e}")
            return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.NOT_SERVING)
        except Exception as e:
            logger.error(f"Health check failed with unexpected error: {e}")
            return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.NOT_SERVING)

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

        # Feature flag scenario - Cache Leak with graceful error handling
        if check_feature_flag("recommendationCacheFailure"):
            span.set_attribute("app.recommendation.cache_enabled", True)
            if random.random() < 0.5 or first_run:
                first_run = False
                span.set_attribute("app.cache_hit", False)
                logger.info("get_product_list: cache miss")
                try:
                    cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=10)
                    response_ids = [x.id for x in cat_response.products]
                    cached_ids = cached_ids + response_ids
                    # Add memory monitoring and cleanup for cache failure simulation
                    cached_ids = cached_ids + cached_ids[:len(cached_ids) // 4]
                    MAX_CACHE_SIZE = 1000  # Prevent unlimited cache growth
                    if len(cached_ids) > MAX_CACHE_SIZE:
                        logger.warning(f"Cache size exceeded {MAX_CACHE_SIZE}, clearing cache")
                        cached_ids = cached_ids[:MAX_CACHE_SIZE // 2]
                    product_ids = cached_ids
                except grpc.RpcError as e:
                    logger.error(f"Product catalog call failed during cache miss: {e}")
                    # Fallback to cached data or default products
                    if cached_ids:
                        logger.info("Falling back to cached product IDs")
                        product_ids = cached_ids
                    else:
                        logger.warning("No cached data available, using default product IDs")
                        product_ids = ['OLJCESPC7Z', 'L9ECAV7KIM', '2ZYFJ3GM2N']  # Default products
                except MemoryError:
                    logger.error("Memory exhaustion detected, clearing cache and falling back")
                    cached_ids = []
                    product_ids = ['OLJCESPC7Z', 'L9ECAV7KIM', '2ZYFJ3GM2N']  # Default products
            else:
                span.set_attribute("app.cache_hit", True)
                logger.info("get_product_list: cache hit")
                product_ids = cached_ids if cached_ids else ['OLJCESPC7Z', 'L9ECAV7KIM', '2ZYFJ3GM2N']
        else:
            span.set_attribute("app.recommendation.cache_enabled", False)
            try:
                cat_response = product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=10)
                product_ids = [x.id for x in cat_response.products]
            except grpc.RpcError as e:
                logger.error(f"Product catalog call failed: {e}")
                # Fallback to default products
                logger.warning("Falling back to default product IDs")
                product_ids = ['OLJCESPC7Z', 'L9ECAV7KIM', '2ZYFJ3GM2N']

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
        logger.error(f'STARTUP_ERROR: {key} environment variable must be set')
        if key == 'OTEL_SERVICE_NAME':
            logger.error(f'Expected format for {key}: string (e.g., "recommendation")')
        elif key == 'PRODUCT_CATALOG_ADDR':
            logger.error(f'Expected format for {key}: host:port (e.g., "product-catalog:3550")')
        elif key == 'RECOMMENDATION_PORT':
            logger.error(f'Expected format for {key}: port number (e.g., "9001")')
        raise Exception(f'{key} environment variable must be set')
    logger.info(f'Environment variable {key} successfully loaded: {value}')
    return value


def check_feature_flag(flag_name: str):
    # Initialize OpenFeature
    client = api.get_client()
    return client.get_boolean_value("recommendationCacheFailure", False)


def validate_dependencies():
    """Validate all required dependencies during startup"""
    try:
        # Test product-catalog connection
        logger.info("Validating product-catalog dependency...")
        pc_channel.get_state(try_to_connect=True)
        # Try a quick health check call
        product_catalog_stub.ListProducts(demo_pb2.Empty(), timeout=10)
        logger.info("Product catalog dependency validated successfully")
        
        # Test flagd connection
        logger.info("Validating flagd dependency...")
        test_flag = api.get_client().get_boolean_value("healthCheck", False)
        logger.info("Flagd dependency validated successfully")
        
        return True
    except grpc.RpcError as e:
        logger.error(f"Product catalog dependency validation failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Dependency validation failed: {e}")
        return False


if __name__ == "__main__":
    # Initialize basic logger first for early error reporting
    logger = logging.getLogger('main')
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    
    service_name = must_map_env('OTEL_SERVICE_NAME')
    api.set_provider(FlagdProvider(host=os.environ.get('FLAGD_HOST', 'flagd'), port=os.environ.get('FLAGD_PORT', 8013)))
    api.add_hooks([TracingHook()])

    # Initialize Traces and Metrics
    tracer = trace.get_tracer_provider().get_tracer(service_name)
    meter = metrics.get_meter_provider().get_meter(service_name)
    rec_svc_metrics = init_metrics(meter)

    # Initialize Logs with OpenTelemetry
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
    otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)

    # Replace basic handler with OTLP handler
    logger.removeHandler(handler)
    logger.addHandler(otel_handler)

    catalog_addr = must_map_env('PRODUCT_CATALOG_ADDR')
    pc_channel = grpc.insecure_channel(catalog_addr)
    product_catalog_stub = demo_pb2_grpc.ProductCatalogServiceStub(pc_channel)

    # Validate dependencies before starting server
    logger.info("Starting dependency validation...")
    if not validate_dependencies():
        logger.error("Startup failed due to dependency validation errors")
        sys.exit(1)
    logger.info("All dependencies validated successfully")

    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add class to gRPC server
    service = RecommendationService()
    demo_pb2_grpc.add_RecommendationServiceServicer_to_server(service, server)
    health_pb2_grpc.add_HealthServicer_to_server(service, server)

    # Start server with error handling
    port = must_map_env('RECOMMENDATION_PORT')
    try:
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        logger.info(f'Recommendation service started successfully, listening on port {port}')
        server.wait_for_termination()
    except Exception as e:
        logger.error(f"Failed to start server on port {port}: {e}")
        sys.exit(1)
