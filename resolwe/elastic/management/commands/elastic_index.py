""".. Ignore pydocstyle D400.

======================
Command: elastic_index
======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder
from resolwe.elastic.mixins import ElasticIndexFilterMixin


class Command(ElasticIndexFilterMixin, BaseCommand):
    """Build ElasticSearch indexes."""

    help = "Build ElasticSearch indexes."

    def handle_index(self, index):
        """Process index."""
        index.build(push=False)
        index.push()

    def handle(self, *args, **options):
        """Command handle."""
        if self.has_filter(options):
            self.filter_indices(options)
        else:
            # Process all indices.
            index_builder.build(push=False)
            index_builder.push()
