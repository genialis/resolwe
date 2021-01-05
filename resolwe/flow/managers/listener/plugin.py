"""Listener plugin and plugin manager class."""
import abc
import logging
from typing import TYPE_CHECKING, Awaitable, Callable, Dict, Optional, Set, Type

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.utils import BraceMessage as __

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor  # noqa: F401


logger = logging.getLogger(__name__)


class ListenerPlugin(metaclass=abc.ABCMeta):
    """Base class for listener plugin classes."""

    # Prefix used in the names of the methods that handle commands.
    handler_prefix = "handle_"

    @classmethod
    def __init_subclass__(cls: Type["ListenerPlugin"], **kwargs):
        """Register class with the registry on initialization."""
        super().__init_subclass__(**kwargs)
        plugin_manager.add_plugin(cls)

    def _get_handler_name(self, command_name: str) -> str:
        """Get the handler name."""
        return f"{self.handler_prefix}{command_name}"

    def get_handler(
        self, command_name: str
    ) -> Optional[Callable[[Message, "Processor"], Response]]:
        """Return the plugin handler.

        When no such plugin exists return None.
        """
        return getattr(self, self._get_handler_name(command_name), None)

    def get_handled_commands(self) -> Set[str]:
        """Return handled command names."""

        return {
            method_name[len(self.handler_prefix) :]
            for method_name in dir(self)
            if (
                method_name.startswith(self.handler_prefix)
                and callable(getattr(self, method_name))
            )
        }


class ListenerPlugins:
    """Class for registering listener plugins."""

    def __init__(self):
        """Initialization."""
        self._plugin_instances: Dict[str, ListenerPlugin] = dict()
        self._command_handlers: Dict[
            str, Callable[[Dict, "Processor"], Awaitable[Dict]]
        ] = dict()
        self._handler_names = set()

    def _get_class_name(self, plugin_class: Type[ListenerPlugin]) -> str:
        """Get the full class name."""
        return "{}.{}".format(plugin_class.__module__, plugin_class.__name__)

    def add_plugin(self, plugin_class: Type[ListenerPlugin]):
        """Add new plugin class.

        :raises AssertionError: when plugin is already registered.
        """
        plugin_name = self._get_class_name(plugin_class)
        assert (
            plugin_name not in self._plugin_instances
        ), "Plugin named {plugin_name} already registered."
        plugin_instance = plugin_class()
        # Sanity check before we register a plugin.
        for handled_command in plugin_instance.get_handled_commands():
            assert (
                handled_command not in self._command_handlers
            ), "Command {handled_command} is already registered"

        for command_name in plugin_instance.get_handled_commands():
            self._command_handlers[command_name] = plugin_instance.get_handler(
                command_name
            )
        self._plugin_instances[plugin_name] = plugin_instance
        logger.debug(__("Listener plugin {} was registered.", plugin_name))

    def get_handler(self, command_name: str) -> Optional[Callable[[Message], Response]]:
        """Return the handler for the given command.

        When such handler exists in multiple plugins one is chosen at random.
        """
        return self._command_handlers.get(command_name)


plugin_manager = ListenerPlugins()
