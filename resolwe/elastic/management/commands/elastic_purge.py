""".. Ignore pydocstyle D400.

======================
Command: elastic_purge
======================

"""
from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder
from resolwe.elastic.mixins import ElasticIndexFilterMixin


class Command(ElasticIndexFilterMixin, BaseCommand):
    """Purge ElasticSearch indexes."""

    help = "Purge ElasticSearch indexes."

    def add_arguments(self, parser):
        """Add an argument to specify output format."""
        super().add_arguments(parser)

        parser.add_argument(
            '--skip-mapping',
            dest='skip_mapping',
            action='store_true',
            help="Don't push fresh mappings to Elasticserch after deleting indices"
        )

    def handle_index(self, index, skip_mapping):
        """Process index."""
        index.destroy()
        if not skip_mapping:
            index.create_mapping()

    def handle(self, *args, **options):
        """Command handle."""
        verbosity = int(options['verbosity'])
        skip_mapping = options['skip_mapping']

        if self.has_filter(options):
            self.filter_indices(options, verbosity, skip_mapping=skip_mapping)
        else:
            # Process all indices.
            index_builder.delete(skip_mapping=skip_mapping)
