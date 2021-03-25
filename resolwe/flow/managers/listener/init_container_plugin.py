"""Commands specific to kubernetes installation."""

from typing import TYPE_CHECKING

from resolwe.flow.executors.socket_utils import Message, Response

from .plugin import ListenerPlugin

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor


class InitContainerPlugin(ListenerPlugin):
    """Handler methods for Init Container."""

    name = "Init container plugin"

    def handle_init_suspend_heartbeat(
        self, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Suspend heartbeat."""
        manager._listener.communicator.suspend_heartbeat(manager.peer_identity)
        return message.respond_ok("")
