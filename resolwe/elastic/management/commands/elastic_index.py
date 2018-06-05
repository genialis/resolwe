""".. Ignore pydocstyle D400.

======================
Command: elastic_index
======================

"""
from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder
from resolwe.elastic.mixins import ElasticIndexFilterMixin


class Command(ElasticIndexFilterMixin, BaseCommand):
    """Build ElasticSearch indexes."""

    help = "Build ElasticSearch indexes."

    def handle_index(self, index):
        """Process index."""
        index.build()

    def handle(self, *args, **options):
        """Command handle."""
        verbosity = int(options['verbosity'])

        if self.has_filter(options):
            self.filter_indices(options, verbosity)
        else:
            # Process all indices.
            index_builder.build()
