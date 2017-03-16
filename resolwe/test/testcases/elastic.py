""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.test.TransactionElasticSearchTestCase
    :members:

.. autoclass:: resolwe.test.ElasticSearchTestCase
    :members:

"""

from __future__ import absolute_import, division, print_function, unicode_literals

from django.test import TestCase as DjangoTestCase
from django.test import TransactionTestCase as DjangoTransactionTestCase
from django.test import override_settings


@override_settings(ELASTICSEARCH_INDEX_PREFIX='test')
class TransactionElasticSearchTestCase(DjangoTransactionTestCase):
    """Class for writing ElasticSearch tests not enclosed in a transaction.

    It is based on Django's :class:`~django.test.TransactionTestCase`.
    Use it if your tests depend on ElasticSearch and you need to access
    the test's database from another thread/process.
    This test class also takes care of cleaning the ElasticSearch data
    before and after each test and prepares a fresh index.

    """

    def setUp(self):
        """Delete any existing data and prepare fresh indexes."""
        from resolwe.elastic.builder import index_builder, IndexBuilder
        # Connect all signals
        from resolwe.elastic import signals

        super(TransactionElasticSearchTestCase, self).setUp()

        index_builder.destroy()  # Delete default indexes

        self._test_index_builder = IndexBuilder()
        # Mock index builder
        signals.index_builder = self._test_index_builder

    def tearDown(self):
        """Delete indexes and data from ElasticSearch."""
        self._test_index_builder.destroy()
        super(TransactionElasticSearchTestCase, self).tearDown()

    def push_indexes(self):
        """Push documents to Elasticsearch."""
        self._test_index_builder.push(index=None)


@override_settings(ELASTICSEARCH_INDEX_PREFIX='test')
class ElasticSearchTestCase(DjangoTestCase, TransactionElasticSearchTestCase):
    """Class for writing ElasticSearch tests.

    It is based on :class:`~resolwe.test.TransactionElasticSearchTestCase`
    and Django's :class:`~django.test.TestCase`.
    The latter encloses the test code in a database transaction that is
    rolled back at the end of the test.

    """

    pass
