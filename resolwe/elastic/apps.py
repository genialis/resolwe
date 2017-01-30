"""Application configuration."""
import sys

from django.apps import AppConfig


class ElasticConfig(AppConfig):
    """Application configuration."""

    name = 'resolwe.elastic'

    def ready(self):
        """Perform application initialization."""
        is_testing = sys.argv[1:2] == ['test']
        if is_testing:
            # Do not register signals and ES indices when testing
            return

        # Connect all signals
        from . import signals  # pylint: disable=unused-variable

        # Register ES indices
        from . builder import index_builder  # pylint: disable=unused-variable
