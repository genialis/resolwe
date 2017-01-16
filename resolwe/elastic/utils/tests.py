""".. Ignore pydocstyle D400.

==================
Elastic Test Utils
==================

.. autoclass:: resolwe.elastic.utils.tests.ElasticSearchTestCase

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.test import TestCase, override_settings

from resolwe.elastic.builder import index_builder


__all__ = ('ElasticSearchTestCase',)


@override_settings(ELASTICSEARCH_INDEX_PREFIX='test_')
class ElasticSearchTestCase(TestCase):
    """Base class for testing ElasticSearch based features.

    This class should be used if tests depends on ElasticSearch. It takes care
    for cleaning data before/after each test and prepare fresh index.
    """

    def setUp(self):
        """Delete any existing data and prepare fresh indexes."""
        index_builder.destroy()  # clean after failed test
        index_builder.build()

    def tearDown(self):
        """Delete existing data from ElasticSearch."""
        index_builder.destroy()
