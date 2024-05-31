"""Utils for working with zeromq."""

import json
import os
from contextlib import suppress
from logging import Logger
from threading import Lock
from typing import Any, Optional, Tuple

import zmq
import zmq.asyncio
from zmq.auth.asyncio import AsyncioAuthenticator

from .socket_utils import BaseCommunicator, PeerIdentity


async def async_zmq_send_data(
    writer: zmq.asyncio.Socket, data: dict, identity: Optional[PeerIdentity] = None
):
    """Send data over socket.

    Notice: when socket is of type ROUTER data must contain key "identity" with
    bytes value which is prepended to the message so it is routed to the right
    recipient.

    :param writer: zeromq socket to send data to.
    :param data: JSON serializable object.
    :param identity: optional identity.
    :raises: exception on failure.
    """
    message = json.dumps(data, default=str).encode()
    if writer.socket_type == zmq.ROUTER:
        assert identity is not None
        await writer.send_multipart([identity, message])
    else:
        await writer.send(message)


async def async_zmq_receive_data(
    reader: zmq.asyncio.Socket,
) -> Tuple[PeerIdentity, Any, Optional[bytes]]:
    """Receive data from the reader.

    The data is expected to be bytes-encoded JSON representation of a Python object.
    Received data is deserialized to a Python object and returned.

    :returns: optional tuple where first element is the identity of the sender
    and the second one is the received message.

    :raises zmq.ZMQError: on receive error.
    """
    user_id = None
    if reader.socket_type == zmq.DEALER:
        identity = str(reader.getsockopt(zmq.IDENTITY)).encode()
        message = await reader.recv(copy=False)
    else:
        received_identity, message = await reader.recv_multipart(copy=False)
        identity = received_identity.bytes
        user_id = str(message["User-Id"]).encode()
    decoded = json.loads(message.bytes.decode())
    return (identity, decoded, user_id)


class ZMQCommunicator(BaseCommunicator):
    """Handles communication over zeromq."""

    def __init__(
        self,
        socket: zmq.asyncio.Socket,
        name: str,
        logger: Logger,
    ):
        """Initialize."""
        super().__init__(
            name,
            logger,
            socket,
            socket,
            async_zmq_send_data,
            async_zmq_receive_data,
        )


class ZMQAuthenticator(AsyncioAuthenticator):
    """The singleton authenticator."""

    _instance = None
    _instance_lock = Lock()
    _instance_pid: int | None = None

    @classmethod
    def has_instance(cls):
        """Check if the instance exists."""
        return not (cls._instance is None or cls._instance_pid != os.getpid())

    @classmethod
    def instance(cls, context=None):
        """Return a global ZMQAuthenticator instance."""
        if not cls.has_instance():
            with cls._instance_lock:
                if not cls.has_instance():
                    cls._instance = cls(context=context)
                    cls._instance_pid = os.getpid()
        return cls._instance

    def start(self):
        """Ignore possible exception when testing."""
        # The is_testing is not available in the executor so it is imported here.
        from resolwe.test.utils import is_testing

        if is_testing():
            with suppress(zmq.error.ZMQError):
                super().start()
        else:
            super().start()
