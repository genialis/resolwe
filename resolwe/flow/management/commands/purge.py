""".. Ignore pydocstyle D400.

=========================
Delete Unreferenced Files
=========================

"""
from django.core.management.base import BaseCommand

from resolwe.flow.utils.purge import purge_all


class Command(BaseCommand):
    """Purge files with no reference in Data objects, and orphaned storages."""

    help = "Purge files with no reference in Data objects, and orphaned storages."

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument('-f', '--force', action='store_true', help="Delete unreferenced files and storages")

    def handle(self, *args, **options):
        """Call :func:`~resolwe.flow.utils.purge.purge_all`."""
        purge_all(delete=options['force'], verbosity=options['verbosity'])
