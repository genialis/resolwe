"""Registry class for storage connectors."""
import logging
from typing import MutableMapping

from .baseconnector import BaseStorageConnector

logger = logging.getLogger(__name__)


class StorageConnectors(MutableMapping[str, BaseStorageConnector]):
    """A class holding configured instances of storage connectors.

    The connector classes are registered automatically via the hook
    __init_subclass__ in the BaseStorageConnector class when connector classes
    are imported.

    This class is implemented as singleton: constructing multiple instances
    of it will always return the same instance.
    """

    __instance = None  #  A single instance of this class

    @classmethod
    def __new__(cls, *args, **kwargs):
        """Create or return singleton."""
        if StorageConnectors.__instance is None:
            StorageConnectors.__instance = super().__new__(cls)
            StorageConnectors.__instance.__setattr__("_connectors", dict())
            StorageConnectors.__instance.__setattr__("_connector_classes", dict())
        return StorageConnectors.__instance

    def _get_connectors_settings(self) -> dict:
        """Return settings for storage connectors.

        This module can also be imported from inside executor special care
        should be taken from where settings are read.
        """
        key = "STORAGE_CONNECTORS"
        try:
            # Try to import settings from executor.
            from executors.global_settings import SETTINGS

            settings = SETTINGS.get(key, {})
        except ImportError:
            # Import settings from Django.
            from resolwe.storage.settings import STORAGE_CONNECTORS as settings

        return settings

    def add_storage_connector_class(self, connector: BaseStorageConnector):
        """Add storage connector class to the registry."""

        def _get_class_name(klass: BaseStorageConnector):
            return "{}.{}".format(klass.__module__, klass.__name__)

        assert issubclass(connector, BaseStorageConnector)
        full_class_name = _get_class_name(connector)
        logger.info("Adding connector {} to register".format(full_class_name))
        self._connector_classes[full_class_name] = connector
        self._add_connectors_from_settings(full_class_name, connector)

    def _add_connectors_from_settings(
        self, full_class_name: str, klass: BaseStorageConnector
    ):
        """Create instances for the connector of the given type.

        Iterate through connectors defined in the settings for the given
        class name, create their instances and add them to the registry.
        """
        connectors_settings = self._get_connectors_settings()
        for connector_name, connector_settings in connectors_settings.items():
            if connector_settings["connector"] == full_class_name:
                # Validate connector config before creating instance.
                for setting_name in klass.REQUIRED_SETTINGS:
                    if setting_name not in connector_settings["config"]:
                        raise Exception(
                            "Setting named {} must be set in connector {} configuration".format(
                                setting_name, connector_name
                            )
                        )
                instance = klass(connector_settings["config"], connector_name)
                self[connector_name] = instance

    def recreate_connectors(self):
        """Recreate connectors from settings."""
        for class_name, class_ in self._connector_classes.items():
            self._add_connectors_from_settings(class_name, class_)

    def __getitem__(self, item):
        """Get implementation."""
        return self._connectors.get(item)

    def __iter__(self):
        """Return iterator."""
        return iter(self._connectors)

    def __contains__(self, name):
        """Get if registry contains object with the given name."""
        return name in self._connectors

    def __len__(self) -> int:
        """Return the number of registered connectors."""
        return len(self._connectors)

    def __setitem__(self, name, connector):
        """Register or update new connector."""
        assert isinstance(connector, BaseStorageConnector)
        self._connectors[name] = connector

    def __delitem__(self, name):
        """Remove registered connector."""
        self._connectors.pop(name)


connectors = StorageConnectors()
