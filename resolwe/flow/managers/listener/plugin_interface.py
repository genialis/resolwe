"""Base classes for plugin managenent."""

import abc
import logging
from typing import Generic, Type, TypeVar

from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)


PluginType = TypeVar("PluginType", bound="Plugin")


class PluginManager(Generic[PluginType], metaclass=abc.ABCMeta):
    """Base class for plugin manager."""

    def __init__(self):
        """Initialize the plugins dictionary."""
        self._plugins: dict[str, PluginType] = dict()

    def add_plugin(self, plugin_class: Type[PluginType]):
        """Add new plugin.

        :raises AssertionError: when plugin with the same identifier is already
            registered.
        """
        plugin_identifier = plugin_class.get_identifier()
        assert (
            plugin_identifier not in self._plugins
        ), f"Plugin '{plugin_identifier}' already registered."
        self._plugins[plugin_identifier] = plugin_class()
        logger.debug(__("Registering plugin '{}'.", plugin_identifier))

    def get_plugin(self, plugin_identifier: str) -> PluginType:
        """Get the plugin.

        :raises KeyError: when plugin is not registered.
        """
        return self._plugins[plugin_identifier]


class PluginMetaClass(abc.ABCMeta):
    """Metaclass for plugin classes."""

    def __new__(cls, name, bases, attrs):
        """Register the plugin when abstract not explicitily set to False."""
        plugin_class = super().__new__(cls, name, bases, attrs)
        if not attrs.get("abstract", False):
            plugin_class.plugin_manager.add_plugin(plugin_class)
        return plugin_class


class Plugin(metaclass=PluginMetaClass):
    """Basic plugin class.

    It defines method that registers the plugin with the manager at the creation.
    """

    # The name of the variable that represents a plugin manager to register with.
    plugin_manager: PluginManager
    abstract = True

    @classmethod
    def get_identifier(cls) -> str:
        """Return the plugin identifier."""
        return "{}.{}".format(cls.__module__, cls.__name__)
