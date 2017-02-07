"""Application configuration."""
import sys

from django.apps import AppConfig


class ElasticConfig(AppConfig):
    """Application configuration."""

    name = 'resolwe.elastic'

    def ready(self):
        """Perform application initialization."""
        is_testing = sys.argv[1:2] == ['test']
        is_migrating = sys.argv[1:2] == ['migrate']
        if is_testing or is_migrating:
            # Do not register signals and ES indices when:
            # * testing - `index_builder` shouldn't be imported to early
            #   in tests
            # * migrating - model instances used during migrations do
            #   not contain the full functionality of models and things
            #   like content types don't work correctly and signals are
            #   not versioned so they are guaranteed to work only with
            #   the last version of the model
            return

        # Connect all signals
        from . import signals  # pylint: disable=unused-variable

        # Register ES indices
        from . builder import index_builder  # pylint: disable=unused-variable
