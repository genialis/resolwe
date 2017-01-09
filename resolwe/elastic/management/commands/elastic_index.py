""".. Ignore pydocstyle D400.

======================
Command: elastic_index
======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.management.base import BaseCommand

from resolwe.elastic.builder import index_builder


class Command(BaseCommand):
    """Build ElasticSearch indexes."""

    help = "Build ElasticSearch indexes."

    def handle(self, *args, **options):
        """Command handle."""
        index_builder.build(push=False)
        index_builder.push()
