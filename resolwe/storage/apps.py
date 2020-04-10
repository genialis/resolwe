"""Application configuration."""
import logging
from contextlib import suppress
from importlib import import_module

from django.apps import AppConfig
from django.conf import settings
from django.utils.translation import ugettext_lazy as _

from resolwe.storage.connectors import connectors
from resolwe.storage.connectors.baseconnector import BaseStorageConnector

logger = logging.getLogger(__name__)


class StorageConfig(AppConfig):
    """Application configuration."""

    name = "resolwe.storage"
    verbose_name = _("Resolwe Storage")

    def _check_connector_settings(self, connector_settings: dict):
        """Validate the storage connector settings in the django config.

        When there exists a section that does not match any known storage
        connector then error is logged.
        """
        for connector_name, connector_settings in connector_settings.items():
            if connector_name not in connectors:
                full_class_name = connector_settings.get("connector")
                class_exists = False
                is_subclass = False
                with suppress(Exception):
                    module_name, class_name = full_class_name.rsplit(".", 1)
                    module = import_module(module_name)
                    class_exists = hasattr(module, class_name)
                    if class_exists:
                        is_subclass = issubclass(
                            getattr(module, class_name), BaseStorageConnector
                        )
                message = "Connector named {} using class {} is not registered.".format(
                    connector_name, full_class_name
                )
                if not class_exists:
                    message += " Class does not exist."
                elif not is_subclass:
                    message += " Class is not a subclass of BaseStorageConnector."
                logger.warning(message)

    def _get_connectors_settings(self) -> dict:
        """Return storage connector settings."""
        return getattr(settings, "STORAGE_CONNECTORS", {})

    def ready(self):
        """Application initialization."""
        self._check_connector_settings(self._get_connectors_settings())
        # Register signals handlers
        from . import signals  # noqa: F401

        return super().ready()
