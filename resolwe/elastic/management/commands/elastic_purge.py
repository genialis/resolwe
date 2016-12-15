""".. Ignore pydocstyle D400.

======================
Command: elastic_purge
======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder


class Command(BaseCommand):
    """Purge ElasticSearch indexes."""

    help = "Purge ElasticSearch indexes."

    def handle(self, *args, **options):
        """Command handle."""
        index_builder.destroy()
