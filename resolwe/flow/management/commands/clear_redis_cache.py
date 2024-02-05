""".. Ignore pydocstyle D400.

=================
Clear Redis Cache
=================

Clear all the Redis Cache.
"""

from django.core.management.base import BaseCommand

from resolwe.flow.managers.listener.redis_cache import redis_cache


class Command(BaseCommand):
    """Run the executor listener."""

    help = "Clear Redis cache."

    def handle(self, *args, **options):
        """Clear the Redis cache."""
        redis_cache.clear()
