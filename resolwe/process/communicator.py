"""Python process communicator."""
import os
import socket
from pathlib import Path
from typing import Any, Optional, Type

from .socket_utils import Message, Response, receive_data, send_data


class Singleton:
    """Decorator class for singleton."""

    def __init__(self, klass: Type):
        """Initialization."""
        self.klass = klass
        self.instance = None

    def __call__(self, *args, **kwargs):
        """Override standard __call__ method."""
        if self.instance is None:
            self.instance = self.klass(*args, **kwargs)
        return self.instance


@Singleton
class PythonProcessCommunicator:
    """Base communicator for communicating with communication container."""

    __instance: Optional[
        "PythonProcessCommunicator"
    ] = None  #  A single instance of this class

    def __init__(self, _socket: socket.SocketType):
        """Initialization."""
        self._socket = _socket

    def send_command(
        self, command_name: str, data: Any, size_bytes: int = 5
    ) -> Response:
        """Send data and return the response.

        :raises AssertionError: on error.
        """
        command = Message.command(command_name, data)
        send_data(self._socket, command.to_dict())
        received = receive_data(self._socket)
        assert received is not None
        return Response.from_dict(received)

    def __getattr__(self, name: str):
        """Call arbitrary command with 'com.command(args)' syntax.

        When attribute is requested that is not known the method is returned
        that will call the ``send_command`` method with the given arguments
        and returt the ``message_data`` of the received answer.
        """

        def call_command(*args):
            if len(args) == 1:
                args = args[0]
            return self.send_command(name, args).message_data

        if name.startswith("_"):
            return None
        else:
            return call_command


def get_communicator():
    """Create and return a communicator instance."""
    SOCKET_TIMEOUT: Optional[int] = None
    if "SOCKET_TIMEOUT" == os.environ:
        SOCKET_TIMEOUT = int(os.environ["SOCKET_TIMEOUT"])
    SOCKETS_PATH = Path(os.environ.get("SOCKETS_VOLUME", "/sockets"))
    PROCESSING_CONTAINER_SOCKET = SOCKETS_PATH / os.environ.get(
        "SCRIPT_SOCKET", "_socket2.s"
    )

    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(os.fspath(PROCESSING_CONTAINER_SOCKET))
    s.settimeout(SOCKET_TIMEOUT)
    return PythonProcessCommunicator(s)


communicator = None
if "RUNNING_IN_CONTAINER" in os.environ:
    communicator = get_communicator()
