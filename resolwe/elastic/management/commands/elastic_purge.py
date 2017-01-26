""".. Ignore pydocstyle D400.

======================
Command: elastic_purge
======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder
from resolwe.elastic.mixins import ElasticIndexFilterMixin


class Command(ElasticIndexFilterMixin, BaseCommand):
    """Purge ElasticSearch indexes."""

    help = "Purge ElasticSearch indexes."

    def handle_index(self, index):
        """Process index."""
        index.destroy()
        index.create_mapping()

    def handle(self, *args, **options):
        """Command handle."""
        if self.has_filter(options):
            self.filter_indices(options)
        else:
            # Process all indices.
            index_builder.delete()
