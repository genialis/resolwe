"""Application configuration."""
from django.apps import AppConfig


class ElasticConfig(AppConfig):
    """Application configuration."""

    name = 'resolwe.elastic'

    def ready(self):
        """Perform application initialization."""
        # Connect all signals.
        from . import signals  # pylint: disable=unused-variable
        from . builder import index_builder  # pylint: disable=unused-variable
