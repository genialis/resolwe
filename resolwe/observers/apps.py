"""Application configuration."""
from django.apps import AppConfig


class BaseConfig(AppConfig):
    """Application configuration."""

    name = "resolwe.observers"

    def ready(self):
        """Application initialization."""
        # Register signals handlers.
        from . import signals  # noqa: F401
