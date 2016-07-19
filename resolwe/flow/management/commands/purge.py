"""
=========================
Delete Unreferenced Files
=========================

"""
from django.core.management.base import BaseCommand

from resolwe.flow.utils.purge import data_purge


class Command(BaseCommand):

    """Purge files with no reference in
    :class:`~resolwe.flow.models.Data` objects.

    """

    help = "Purge files with no reference in Data objects."

    def add_arguments(self, parser):
        parser.add_argument('-d', '--data', type=str, nargs='+', help="list of data ids")
        parser.add_argument('-f', '--force', action='store_true', help="delete unreferenced files")

    def handle(self, *args, **options):
        """Call :func:`~resolwe.flow.utils.purge.data_purge` to purge
        files.

        """
        data_purge(options['data'], options['force'])
