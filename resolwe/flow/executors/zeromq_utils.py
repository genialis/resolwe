"""Utils for working with zeromq."""
import json
from logging import Logger
from typing import Any, Optional, Tuple

import zmq
import zmq.asyncio

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
) -> Optional[Tuple[PeerIdentity, Any]]:
    """Receive data from the reader.

    The data is expected to be bytes-encoded JSON representation of a Python object.
    Received data is deserialized to a Python object and returned.

    :returns: optional tuple where first element is the identity of the sender
    and the second one is the received message.

    :raises zmq.ZMQError: on receive error.
    """
    if reader.socket_type == zmq.DEALER:
        identity = reader.getsockopt(zmq.IDENTITY)
        message = await reader.recv()
    else:
        identity, message = await reader.recv_multipart()
    decoded = json.loads(message.decode())
    return (identity, decoded)


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
