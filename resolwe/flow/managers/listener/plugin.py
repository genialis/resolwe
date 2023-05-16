"""Listener plugin and plugin manager class."""

import logging
from typing import TYPE_CHECKING, Callable, Optional, Type

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.managers.listener.plugin_interface import Plugin, PluginManager

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor  # noqa: F401

    DataId = int
    HandlerType = Callable[[DataId, Message, "Processor"], Response]


logger = logging.getLogger(__name__)


class ListenerPlugins(PluginManager["ListenerPlugin"]):
    """Class for registering listener plugins."""

    def __init__(self):
        """Create the handlers mappings and plugins dict."""
        super().__init__()
        self._command_handlers: dict[str, HandlerType] = dict()

    def add_plugin(self, plugin_class: Type["ListenerPlugin"]):
        """Add new plugin class.

        :raises AssertionError: when plugin command is already registered.
        """
        # Sanity check before we register a plugin.
        plugin_instance = plugin_class()
        registered_commands = self._command_handlers.keys()
        plugin_commands = plugin_instance.get_handled_commands()
        overlapping = plugin_commands.intersection(registered_commands)
        assert not overlapping, f"Commands '{overlapping}' already registered."

        super().add_plugin(plugin_class)
        for command_name in plugin_instance.get_handled_commands():
            handler = plugin_instance.get_handler(command_name)
            assert handler is not None
            self._command_handlers[command_name] = handler

    def get_handler(self, command_name: str) -> Optional["HandlerType"]:
        """Return the handler for the given command.

        When such handler exists in multiple plugins one is chosen at random.
        """
        return self._command_handlers.get(command_name)


listener_plugin_manager = ListenerPlugins()


class ListenerPlugin(Plugin):
    """Base class for listener plugin classes."""

    # Prefix used in the names of the methods that handle commands.
    handler_prefix = "handle_"
    manager_name = "listener_plugin_manager"
    abstract = True

    def _get_handler_name(self, command_name: str) -> str:
        """Get the handler name."""
        return f"{self.handler_prefix}{command_name}"

    def get_handler(self, command_name: str) -> Optional["HandlerType"]:
        """Return the plugin handler.

        When no such plugin exists return None.
        """
        return getattr(self, self._get_handler_name(command_name), None)

    def get_handled_commands(self) -> set[str]:
        """Return handled command names."""

        return {
            method_name[len(self.handler_prefix) :]
            for method_name in dir(self)
            if (
                method_name.startswith(self.handler_prefix)
                and callable(getattr(self, method_name))
            )
        }
