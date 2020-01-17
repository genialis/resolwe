""".. Ignore pydocstyle D400.

========================
Command: elastic_mapping
========================

"""
from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder
from resolwe.elastic.mixins import ElasticIndexFilterMixin


class Command(ElasticIndexFilterMixin, BaseCommand):
    """Create ElasticSearch mappings."""

    help = "Create ElasticSearch mappings."

    def handle_index(self, index):
        """Process index."""
        index.create_mapping()

    def handle(self, *args, **options):
        """Command handle."""
        verbosity = int(options["verbosity"])

        if self.has_filter(options):
            self.filter_indices(options, verbosity)
        else:
            # Process all indices.
            index_builder.create_mappings()
