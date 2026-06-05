# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import logging
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import recommendation_server


class FakeCatalogStub:
    def __init__(self, product_ids):
        self._product_ids = product_ids

    def ListProducts(self, _):
        return SimpleNamespace(
            products=[SimpleNamespace(id=product_id) for product_id in self._product_ids]
        )


class DummySpan:
    def set_attribute(self, *_):
        return None


class DummyTracer:
    @contextmanager
    def start_as_current_span(self, _):
        yield DummySpan()


class RecommendationCacheBoundTest(unittest.TestCase):
    def setUp(self):
        recommendation_server.cached_ids = []
        recommendation_server.first_run = True
        recommendation_server.tracer = DummyTracer()
        recommendation_server.logger = logging.getLogger("recommendation-test")
        recommendation_server.product_catalog_stub = FakeCatalogStub(
            [f"id-{index}" for index in range(2000)]
        )

    def test_cache_growth_is_capped_when_failure_flag_is_enabled(self):
        with (
            patch("recommendation_server.check_feature_flag", return_value=True),
            patch("recommendation_server.random.random", return_value=0.0),
        ):
            for _ in range(12):
                recommendation_server.get_product_list(["id-1", "id-2"])

        self.assertEqual(
            len(recommendation_server.cached_ids),
            recommendation_server.MAX_CACHED_IDS,
        )


if __name__ == "__main__":
    unittest.main()
