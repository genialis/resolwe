"""Utility methods for receiving/sending data over socket."""
import asyncio
import functools
import json
import logging
import random
import socket
import time
import uuid
from collections import deque
from contextlib import suppress
from enum import Enum, unique
from time import time as now
from typing import (
    Any,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generic,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

logger = logging.getLogger(__name__)


@unique
class MessageType(Enum):
    """Type of the message."""

    COMMAND = "COMMAND"
    RESPONSE = "RESPONSE"
    HEARTBEAT = "HBT"
    HEARTBEAT_RESPONSE = "HBTR"
    ENQUIRE = "EQR"


class ResponseStatus(Enum):
    """Response status."""

    OK = "OK"
    ERROR = "ERR"


class PeerStatus(Enum):
    """Peer status."""

    RESPONSIVE = "responsive"
    DEGRADED = "degraded"
    UNRESPONSIVE = "unresponsive"


PeerIdentity = bytes
MessageDataType = TypeVar("MessageDataType")
ResponseDataType = TypeVar("ResponseDataType")


def retry(
    max_retries: int = 3,
    retry_exceptions: Tuple[Type[Exception]] = (ConnectionError,),
    min_sleep: int = 1,
    max_sleep: int = 10,
):
    """Try to call decorated method max_retries times before giving up.

    The calls are retried when function raises exception in retry_exceptions.

    :param max_retries: maximal number of calls before giving up.
    :param retry_exceptions: retry call if one of these exceptions is raised.
    :param min_sleep: minimal sleep between calls (in seconds).
    :param max_sleep: maximal sleep between calls (in seconds).
    :returns: return value of the called method.
    :raises: the last exceptions raised by the method call if none of the
      retries were successfull.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            last_error: Exception = Exception("Retry failed")
            sleep: int = 0
            for retry in range(max_retries):
                try:
                    time.sleep(sleep)
                    return func(*args, **kwargs)
                except retry_exceptions as err:
                    sleep = min(max_sleep, min_sleep * (2 ** retry))
                    last_error = err
            raise last_error

        return wrapper_retry

    return decorator_retry


def async_retry(
    max_retries: int = 3,
    retry_exceptions: Tuple[Type[Exception]] = (ConnectionError,),
    min_sleep: int = 1,
    max_sleep: int = 10,
):
    """Try to call decorated method max_retries times before giving up.

    The calls are retried when function raises exception in retry_exceptions.

    :param max_retries: maximal number of calls before giving up.
    :param retry_exceptions: retry call if one of these exceptions is raised.
    :param min_sleep: minimal sleep between calls (in seconds).
    :param max_sleep: maximal sleep between calls (in seconds).
    :returns: return value of the called method.
    :raises: the last exceptions raised by the method call if none of the
      retries were successfull.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        async def wrapper_retry(*args, **kwargs):
            last_error: Exception = Exception("Retry failed")
            sleep: int = 0
            for retry in range(max_retries):
                try:
                    await asyncio.sleep(sleep)
                    return await func(*args, **kwargs)
                except retry_exceptions as err:
                    sleep = min(max_sleep, min_sleep * (2 ** retry))
                    last_error = err
            raise last_error

        return wrapper_retry

    return decorator_retry


def send_data(
    s: socket.SocketType,
    data: dict,
    size_bytes: int = 8,
    encoder: Optional[Type[json.JSONEncoder]] = None,
):
    """Send data over socket.

    :param s: socket to send over.
    :param data: dict that must be serialzable to JSON.
    :param size_bytes: how first many bytes in message are dedicated to the
        message size (pre-padded with zeros).
    :raises: exception on failure.
    """
    message = json.dumps(data, cls=encoder).encode()
    message_length = len(message).to_bytes(size_bytes, byteorder="big")
    s.sendall(message_length)
    s.sendall(message)


def read_bytes(s: socket.SocketType, message_size: int) -> bytes:
    """Read message_size bytes from the given socket.

    The method will block until enough bytes are available.

    :param message_size: size (in bytes) of the message to read.

    :returns: received message.
    """
    message = b""
    while len(message) < message_size:
        received = s.recv(message_size - len(message))
        message += received
        if not received:
            return message
    return message


def receive_data(s: socket.SocketType, size_bytes: int = 8) -> Optional[Dict]:
    """Recieve data over the given socket.

    :param size_bytes: how first many bytes in message are dedicated to the
        message size (pre-padded with zeros).
    """
    message = read_bytes(s, size_bytes)
    if not message:
        return None
    message_size = int.from_bytes(message, byteorder="big")

    message = read_bytes(s, message_size)
    assert len(message) == message_size
    data = json.loads(message.decode("utf-8"))
    return data


async def async_send_data(
    writer: asyncio.StreamWriter,
    data: dict,
    identity: Optional[bytes] = None,
    size_bytes: int = 8,
):
    """Send data over socket.

    :param writer: object to write into.
    :param data: JSON serializable object.
    :param identity: ignored by socker writer.
    :param size_bytes: how first many bytes in message are dedicated to the
        message size (pre-padded with zeros).
    :raises: exception on failure.
    """
    message = json.dumps(data).encode("utf-8")
    message_size = len(message).to_bytes(size_bytes, byteorder="big")
    writer.write(message_size)
    writer.write(message)
    await writer.drain()


async def async_receive_data(
    reader: asyncio.StreamReader, size_bytes: int = 8
) -> Optional[Tuple[PeerIdentity, Any]]:
    """Receive data from the reader.

    The data is expected to be bytes-encoded JSON representation of a Python
    object. Received data is deserialized to a Python object.

    :raises asyncio.IncompleteReadError: when data could not be read from
        the socket.

    :returns: optional tuple where first element is identity of the messenger
        (None with socket) and the second tuple is the received message.
    """
    received = await reader.readexactly(size_bytes)
    message_size = int.from_bytes(received, byteorder="big")
    received = await reader.readexactly(message_size)
    assert len(received) == message_size
    return (b"", json.loads(received.decode("utf-8")))


class Message(Generic[MessageDataType]):
    """Base class for message."""

    def __init__(
        self,
        message_type: MessageType,
        type_data: str,
        message_data: MessageDataType,
        message_uuid: Optional[str] = None,
        sequence_number: Optional[int] = None,
        sent_timestamp: Optional[float] = None,
    ):
        """Initialize.

        The sent_timestamp is auto-set on sending message to timestamp.
        """
        self.message_type = message_type
        self.message_data = message_data
        self.type_data = type_data
        self.uuid = message_uuid or self._get_random_message_identifier()
        self.sequence_number = sequence_number or 1
        self.sent_timestamp = sent_timestamp

    def _get_random_message_identifier(self) -> str:
        """Get a random message identifier.

        This identifier is uses to match a response with the request.
        """
        return uuid.uuid4().hex

    @property
    def response_status(self) -> ResponseStatus:
        """Get response status if message type is ``MessageType.RESPONSE``.

        :raises ValueError: when type_data is not one of value in
            ResponseStatus enum.

        :raises AssertionError: if type is not ``MessageType.RESPONSE``.
        """
        assert self.message_type is MessageType.RESPONSE
        return ResponseStatus(self.type_data)

    @property
    def command_name(self) -> str:
        """Get response status if message type is ``MessageType.RESPONSE``.

        :raises AssertionError: if type is not ``MessageType.RESPONSE``.
        """
        assert self.message_type is MessageType.COMMAND
        return self.type_data

    def respond(
        self,
        response_data: ResponseDataType,
        response_status: ResponseStatus = ResponseStatus.OK,
    ) -> "Response[ResponseDataType]":
        """Return a response with the given data and status."""
        return Response(
            response_status.value,
            response_data,
            self.uuid,
            self.sequence_number,
        )

    def respond_heartbeat(self) -> "Response[str]":
        """Respond heartbeat messages."""
        response = self.respond_ok("")
        response.message_type = MessageType.HEARTBEAT_RESPONSE
        return response

    def respond_ok(
        self, response_data: ResponseDataType
    ) -> "Response[ResponseDataType]":
        """Respond OK with the given data."""
        return self.respond(response_data, ResponseStatus.OK)

    def respond_error(
        self, response_data: ResponseDataType
    ) -> "Response[ResponseDataType]":
        """Respond ERROR with the given data."""
        return self.respond(response_data, ResponseStatus.ERROR)

    @staticmethod
    def command(
        command_name: str,
        message_data: MessageDataType,
        message_uuid: Optional[str] = None,
        sequence_number: Optional[int] = None,
    ) -> "Message[MessageDataType]":
        """Construct and return a command."""
        return Message(
            MessageType.COMMAND,
            command_name,
            message_data,
            message_uuid,
            sequence_number,
        )

    @staticmethod
    def heartbeat() -> "Message[str]":
        """Construct the heartbeat message."""
        return Message(MessageType.HEARTBEAT, "", "")

    @staticmethod
    def is_valid(message_dict: Dict) -> bool:
        """Validate the dictionary representing the message."""
        try:
            required_keys = ("type", "type_data", "data", "uuid", "sequence_number")
            value_types: Tuple = (str, str, object, str, int)
            assert set(required_keys).issubset(set(message_dict.keys()))
            assert all(
                isinstance(message_dict[key], value_type)
                for key, value_type in zip(required_keys, value_types)
            )
            message_type = MessageType(message_dict["type"])
            # If message is response chech that type_data is of the correct type.
            if message_type is MessageType.RESPONSE:
                ResponseStatus(message_dict["type_data"])
        except (AssertionError, ValueError):
            return False
        else:
            return True

    @staticmethod
    def from_dict(message_dict: Dict) -> "Message":
        """Construct a Message from dictionary.

        :raises KeyError, ValueError: if dictionary does not represent the
            valid message.
        """
        if message_dict["type"] == MessageType.RESPONSE.value:
            return Response.from_dict(message_dict)
        return Message(
            MessageType(message_dict["type"]),
            message_dict["type_data"],
            message_dict["data"],
            message_dict.get("uuid"),
            message_dict.get("sequence_number"),
            message_dict.get("timestamp"),
        )

    def to_dict(self) -> dict:
        """Get representation of the message as a dictionary.

        It is suitable for serialization into JSON.
        """
        return {
            "type": self.message_type.value,
            "type_data": self.type_data,
            "data": self.message_data,
            "uuid": self.uuid,
            "sequence_number": self.sequence_number,
            "timestamp": self.sent_timestamp,
        }

    def __repr__(self) -> str:
        """Return the string representation."""
        return str(self.to_dict())

    def __eq__(self, other):
        """Equality check."""
        if isinstance(other, Message):
            return (
                self.type_data == other.type_data
                and self.message_data == other.message_data
                and self.message_type == other.message_type
            )
        return False


class Response(Message[MessageDataType]):
    """Response message."""

    def __init__(
        self,
        type_data: str,
        message_data: MessageDataType,
        message_uuid: Optional[str] = None,
        sequence_number: Optional[int] = None,
        sent_timestamp: Optional[float] = None,
    ):
        """Initialize."""
        super().__init__(
            MessageType.RESPONSE,
            type_data,
            message_data,
            message_uuid,
            sequence_number,
            sent_timestamp,
        )
        self.status: ResponseStatus = ResponseStatus(type_data)

    @staticmethod
    def from_dict(message_dict: Dict) -> "Response":
        """Construct a Response from dictionary.

        :raises KeyError, ValueError: if dictionary does not represent the
            valid message.
        """
        assert message_dict["type"] == MessageType.RESPONSE.value
        return Response(
            message_dict["type_data"],
            message_dict["data"],
            message_dict.get("uuid"),
            message_dict.get("sequence_number"),
            message_dict.get("timestamp"),
        )


class MessageStatus(Enum):
    """Status of the message."""

    RECEIVED = "received"
    UNKNOWN = "unknown"


class EventWithResponse(asyncio.Event):
    """Event class with response property."""

    def __init__(self):
        """Initialize."""
        super().__init__()
        self._response: Optional[Any] = None

    @property
    def response(self) -> Any:
        """Return the response.

        :raises RuntimeError: when response is not set.
        """
        if self._response is None:
            raise RuntimeError("Response is not set")

        return self._response

    @response.setter
    def response(self, message_data: Any):
        self._response = message_data


class BaseCommunicator:
    """Handles socket communication.

    The communication is always iniciated by the command which must be followed
    by the response. The command is the JSON representation (encoded to bytes)
    of the dictionary of the following format::

        {
            "command": "command_name",
            "data": additional_command_data,
            "uuid": unique_command_id
        }

    The response to the command is also JSON representation (encoded to bytes)
    of the dictionary of the following format::

        {
            "response": "OK/ERROR",
            "data": additional_response_data,
            "uuid": unique_command_id
        }

    , where ``uuid`` in the response must match the one in the command.

    This class has two basic methods: one for sending commands and one for
    reponding to them. The method that receives commands must be run as a
    separate task.

    To cleanly shutdown the receiving task one must call a
    ``terminate`` method and wait for the receiving task to finish.
    """

    FINAL_COMMAND_NAME = "finish"

    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        reader: Any,
        writer: Any,
        send_method: Callable,
        receive_method: Callable,
        peer_status_changed: Optional[
            Callable[[PeerIdentity, PeerStatus], Coroutine]
        ] = None,
    ):
        """Initialize.

        :param name: used in logging to identify the communicator.
        :param logger: logger to use for logging.
        :param reader: object to use when receiving messages.
        :param writer: object to use when sending messages.
        :param send_method: method to use when sending messages.
        :param receive_method: method to use when receiving messages.

        """
        self.name = name
        self.logger = logger
        self._terminating = asyncio.Event()
        self.has_message = asyncio.Event()
        self.send_method = send_method
        self.receive_method = receive_method
        self.reader = reader
        self.writer = writer

        self._listening_future: Optional[asyncio.Future] = None
        # Wait for x seconds for listening future to complete when _terminating
        # flag is set.
        self._listening_future_wait_timeout = 30

        self.sequence_numbers: MutableMapping[PeerIdentity, int] = dict()
        self._command_queue: Deque[Tuple[PeerIdentity, Message]] = deque()

        self._uuid_to_event: Dict[str, EventWithResponse] = dict()

        # Keep two lists of received messages. After 10 minutes move current
        # messages to older list and discard older messages. So the list
        # of received messages will not become too large.
        self._uuids_received: Set[str] = set()
        self._uuids_received_old: Set[str] = set()
        # When no message has been exchanged with the peer for
        # _heartbeat_interval seconds the heartbeat message is sent.
        self._heartbeat_interval = 120

        # After that many unanswered heartbeats the peer is removed from the
        # _known_peers list and heartbeats are no longer sent to it.
        self._max_heartbeats_skipped = 5
        # Mapping from peer identity to the timestamp of the last message
        # received by the peer.
        self._known_peers: Dict[PeerIdentity, float] = dict()
        self._degraded_peers: Set[PeerIdentity] = set()
        self._last_heartbeat: Dict[PeerIdentity, float] = dict()
        self.peer_status_changed = peer_status_changed
        self._heartbeat_messages_future: Optional[asyncio.Future] = None
        self._watchdog_future: Optional[asyncio.Future] = None
        self._recycle_uuids_future: Optional[asyncio.Future] = None

    def __getattr__(self, name: str):
        """Call arbitrary 'command' with 'communicator.command(args)' syntax."""

        def call_command(*args):
            if len(args) == 1:
                args = args[0]
            return self.send_command(Message.command(name, args))

        if name.startswith("_"):
            return None
        else:
            return call_command

    async def _recycle_received_uuids(self, timeout: int = 600):
        """Recycle the list of received messages uuids.

        Move received messages to the old list and discard old ones.
        """
        while True:
            self._uuids_received_old = self._uuids_received
            self._uuids_received = set()
            await asyncio.sleep(timeout)

    async def _receive_message(self) -> Optional[Tuple[PeerIdentity, Message]]:
        """Receive a single message.

        This method is blocking: it waits for the message to arrive. The
        method return only when one of the following happens:

        1. The message is received. In this case the message is returned.

        2. The terminating flag is set. In this case None is returned.

        3. Unexpected error occurs when receiving message. In this case None
           is returned.

        :returns: received message or None if message could not be received.
            The returned message is guaranteed to be dictionary and contain
            the key "type".
        """
        result = None
        try:
            self.logger.debug("Communicator %s waiting for message.", self.name)
            receive_task = asyncio.ensure_future(self.receive_method(self.reader))
            terminating_task = asyncio.ensure_future(self._terminating.wait())
            done, pending = await asyncio.wait(
                (receive_task, terminating_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if receive_task in done:
                try:
                    received = receive_task.result()
                except asyncio.exceptions.IncompleteReadError:
                    self.logger.info("Socket closed by peer, stopping communication.")
                    received = None
                if received is not None:
                    assert isinstance(received, tuple)
                    assert len(received) == 2
                    assert isinstance(received[0], bytes)
                    assert Message.is_valid(received[1])
                    result = received[0], Message.from_dict(received[1])
            else:
                self.logger.debug(
                    "Communicator %s _receive_message: terminating flag is set, returning None",
                    self.name,
                )
        except:
            self.logger.exception(
                "Communicator %s: exception in _receive_message.", self.name
            )
        finally:
            # Always stop both tasks.
            receive_task.cancel()
            terminating_task.cancel()
        return result

    async def _send_message(
        self,
        message: Message,
        identity: PeerIdentity = b"",
        send_timeout: Optional[float] = 6,
        send_retries: int = 10,
    ) -> Any:
        """Send message using the supplied method.

        If message does not contains key "uuid" one is chosen at random.

        :param message: the message to send.

        :param identity: the identity of the peer to send message to.

        :param send_timeout: timeout (in seconds) to wait for the reply. If
            its value is None the timeout is infinite. This timeout will
            probably never occur since 0mq has internal buffer for sending
            messages.

        :param send_retries: try to resend command this many times if error
            occurs when sending.

        :returns: the result of the sending method call (usually None).

        :raises RuntimeError: when user supplied method raises exception when
            sending message or timeout occured when sending message.
        """
        retries = 0
        while retries < send_retries:
            try:
                message.sent_timestamp = now()
                return await asyncio.wait_for(
                    self.send_method(self.writer, message.to_dict(), identity),
                    timeout=send_timeout,
                )
            except asyncio.TimeoutError:
                self.logger.error(
                    f"Communicator {self.name}: sending message timeout (retry {retries+1}/{send_retries})."
                )
            except asyncio.CancelledError:
                self.logger.info(
                    f"Communicator {self.name}: sending message was canceled."
                )
                return
            except Exception:
                self.logger.exception(
                    f"Communicator {self.name}: exception while sending message (retry {retries+1}/{send_retries})."
                )
            finally:
                retries += 1
        raise RuntimeError(f"Sending message ({message}, {str(identity)}) timeout.")

    async def __aenter__(self):
        """Start listening for messages on entering context."""
        assert self._listening_future is None
        assert self._heartbeat_messages_future is None
        assert self._watchdog_future is None
        assert self._recycle_uuids_future is None

        self._terminating.clear()

        self._listening_future = asyncio.ensure_future(self.start_listening())
        self._heartbeat_messages_future = asyncio.ensure_future(
            self._send_heartbeat_messages()
        )
        self._watchdog_future = asyncio.ensure_future(self._watchdog())
        self._recycle_uuids_future = asyncio.ensure_future(
            self._recycle_received_uuids()
        )

        self.logger.debug("Communicator %s: entering context.", self.name)
        return self

    async def __aexit__(self, typ, value, trace):
        """On exiting a context, stop listening for messages."""
        self.logger.debug(f"Communicator {self.name}: leaving context.")
        assert self._heartbeat_messages_future is not None
        assert self._watchdog_future is not None
        assert self._listening_future is not None
        assert self._recycle_uuids_future is not None

        self._terminating.set()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._listening_future, timeout=self._listening_future_wait_timeout
            )
        self._watchdog_future.cancel()
        self._heartbeat_messages_future.cancel()
        self._recycle_uuids_future.cancel()
        self._watchdog_future = None
        self._listening_future = None
        self._listening_future = None
        self.logger.debug("Communicator %s: leaving context.", self.name)

    def _message_status(self, message_uuid: str) -> MessageStatus:
        """Get status of message with the given UUID."""
        self.logger.debug("Checking for message status: %s.", message_uuid)
        if (
            message_uuid in self._uuids_received
            or message_uuid in self._uuids_received_old
        ):
            return MessageStatus.RECEIVED
        else:
            return MessageStatus.UNKNOWN

    async def _status_changed(self, peer: PeerIdentity, status: PeerStatus):
        """Notify (if possible) of the peer status change."""
        if self.peer_status_changed is not None:
            try:
                await self.peer_status_changed(peer, status)
            except:
                self.logger.exception("Exception in peer_status_changed.")

    async def _get_peer_message_status(
        self, message_uuid: str, identity: bytes = b"", response_timeout: int = 300
    ) -> str:
        """Enquire peer for the message status.

        :raises RuntimeError: on failure.
        """
        result = await self.send_command(
            Message(MessageType.ENQUIRE, "", message_uuid),
            peer_identity=identity,
            response_timeout=response_timeout,
        )
        return result.message_data

    async def _send_heartbeat_messages(self):
        """Send heartbeat messages when necessary.

        This coroutine never stops and must be cancelled by the parent.
        """
        while True:
            try:
                self.logger.debug(
                    "Heartbeat checking known peers %s.", list(self._known_peers.keys())
                )
                # Wait a little longer than heartbeat interval if there is nothing to do.
                min_sleep_interval = self._heartbeat_interval + 5
                for identity, last_received in self._known_peers.items():
                    # Count time from last received message or last sent heartbeat.
                    last_event = max(
                        last_received, self._last_heartbeat.get(identity, last_received)
                    )
                    logger.debug(
                        "Last seen %s: %d seconds ago",
                        identity,
                        now() - last_received,
                    )
                    # Add a little random noise to spread the heartbeats around.
                    sleep_interval = int(
                        last_event
                        + self._heartbeat_interval
                        + random.uniform(0, 5 + 1)
                        - now()
                    )
                    logger.debug("Heartbeat in: %d seconds.", sleep_interval)

                    if sleep_interval <= 0:
                        asyncio.ensure_future(
                            self._send_message(Message.heartbeat(), identity=identity)
                        )
                        self._last_heartbeat[identity] = now()
                    else:
                        min_sleep_interval = min(min_sleep_interval, sleep_interval)
                self.logger.debug(
                    "Next heartbeat check: in %d seconds.", min_sleep_interval
                )
                await asyncio.sleep(min_sleep_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Unexpected exception while sending heartbeats.")

    def suspend_heartbeat(self, peer_identity: PeerIdentity):
        """Temporary suspend heartbeat from the peer.

        The heartbeats are resumed on the first message received from the peer.
        """
        self._last_heartbeat.pop(peer_identity, None)
        self._known_peers.pop(peer_identity, None)

    async def _watchdog(self):
        """Watches and reports peer status changes."""
        try:
            to_remove_interval = int(
                self._max_heartbeats_skipped * self._heartbeat_interval
                + self._heartbeat_interval / 2
            )
            while True:
                failed_peers = set()
                recovered_peers = set()
                degraded_peers = set()
                sleep_interval = self._heartbeat_interval + 5
                # Check all peers for which heartbeats were sent and not responded.
                for identity, timestamp in self._last_heartbeat.items():
                    last_seen = self._known_peers[identity]
                    self.logger.debug(
                        "Watchdog (%s) observing peer %s", self.name, identity
                    )
                    self.logger.debug("Last peer activity: %s.", last_seen)
                    self.logger.debug("Last peer heartbeat sent: %s.", timestamp)
                    self.logger.debug("Since last ping: %d", now() - timestamp)
                    self.logger.debug("Since last seen: %d", now() - last_seen)
                    self.logger.debug(
                        "Until to remove: %d", last_seen + to_remove_interval - now()
                    )
                    self.logger.debug("To remove interval: %d", to_remove_interval)
                    # Message received after heartbeat was sent.
                    # If peer was in degraded state before move it to recovered.
                    if last_seen > timestamp:
                        recovered_peers.add(identity)
                    # Peer is unresponsive.
                    elif now() - last_seen > to_remove_interval:
                        failed_peers.add(identity)
                        continue
                    # Peer failed to answer for 2 heartbeat intervals, entering
                    # degraded state.
                    elif (
                        now() - last_seen > 2 * self._heartbeat_interval
                        and identity not in self._degraded_peers
                    ):
                        degraded_peers.add(identity)

                    till_next_heartbeat = int(
                        timestamp + self._heartbeat_interval - now()
                    )
                    till_remove = int(last_seen + to_remove_interval - now())

                    sleep_interval = min(
                        sleep_interval, till_remove, till_next_heartbeat
                    ) + random.uniform(2, 5)
                self.logger.debug(
                    "Watchdog (%s) adjusting sleep interval: %d",
                    self.name,
                    sleep_interval,
                )

                for recovered_peer in recovered_peers:
                    del self._last_heartbeat[recovered_peer]
                    if recovered_peer in self._degraded_peers:
                        self.logger.debug("Peer %s recovering.", recovered_peer)
                        self._degraded_peers.remove(recovered_peer)
                        await self._status_changed(
                            recovered_peer, PeerStatus.RESPONSIVE
                        )
                for failed_peer in failed_peers:
                    self.logger.debug(
                        "Peer %s entering unresponsive state.", failed_peer
                    )
                    del self._known_peers[failed_peer]
                    del self._last_heartbeat[failed_peer]
                    await self._status_changed(failed_peer, PeerStatus.UNRESPONSIVE)

                for degraded_peer in degraded_peers:
                    self.logger.debug("Peer %s entering degraded state.", degraded_peer)
                    self._degraded_peers.add(degraded_peer)
                    await self._status_changed(degraded_peer, PeerStatus.DEGRADED)
                self.logger.debug(
                    "Watchdog (%s) sleeping for %d seconds.", self.name, sleep_interval
                )
                await asyncio.sleep(sleep_interval)
        except asyncio.CancelledError:
            logger.info("Watchdog shutting down.")
        except:
            logger.exception("Unhandled exception in watchdog.")

    async def start_listening(self):
        """Start listening for messages.

        There are two types of messages: commands and responses.

        When a command is received a command_handler callback is called and
        the response returned by it is sent back to the peer.

        A response is first paired with the sent command and the Event object
        is set so that response is received in the send_command method.
        """
        try:
            while True:
                received = await self._receive_message()

                if received is None:
                    self.logger.info(
                        f"Communicator {self.name}: received empty message, closing communicator."
                    )
                    break

                identity, message = received

                # Only add to known peers if the identity is an integer.
                # When executor communicates with listener (sending logs for
                # instance) in uses identity contianing letters. Such should
                # should not be added to known_peers dict since we do not have
                # to monitor them using watchdog.
                with suppress(ValueError):
                    int(identity)
                    if identity not in self._known_peers:
                        self.logger.debug("Adding new peer with identity %s.", identity)
                        self._known_peers[identity] = now()

                if message.message_type is MessageType.HEARTBEAT:
                    # Send response in the background.
                    asyncio.ensure_future(
                        self._send_message(
                            message.respond_heartbeat(), identity=identity
                        )
                    )

                elif message.message_type is MessageType.HEARTBEAT_RESPONSE:
                    if identity in self._last_heartbeat:
                        del self._last_heartbeat[identity]
                    else:
                        self.logger.warning(
                            "The identity %s sent unknown heartbear response.", identity
                        )

                elif message.message_type is MessageType.ENQUIRE:
                    # Send response in the background.
                    asyncio.ensure_future(
                        self._send_message(
                            message.respond(
                                self._message_status(message.message_data).value
                            ),
                            identity=identity,
                        )
                    )

                elif message.message_type is MessageType.COMMAND:
                    if message.uuid not in self._uuids_received:
                        # If final command is received remove the peer from
                        # the list of known peers.
                        if message.command_name == self.FINAL_COMMAND_NAME:
                            if identity in self._known_peers:
                                del self._known_peers[identity]
                            if identity in self._last_heartbeat:
                                del self._last_heartbeat[identity]
                        self._command_queue.append((identity, message))
                        self.logger.debug("New message has arrived")
                        self.logger.debug(
                            f"Number of messages in queue: {len(self._command_queue)}"
                        )

                        self.has_message.set()

                elif message.message_type is MessageType.RESPONSE:
                    # Response from the peer. Match it with the command.
                    response_event = self._uuid_to_event.get(message.uuid, None)
                    if response_event is None:
                        self.logger.error(
                            f"Communicator {self.name}: Can not match response {message} with sent command."
                        )
                    else:
                        response_event.response = message
                        response_event.set()

                else:
                    # Unknown message received. Log it and continue.
                    self.logger.error(
                        f"Communicator {self.name} got unknown message: {message}."
                    )

                self._uuids_received.add(message.uuid)

        except Exception:
            self.logger.exception("Exception while listening for messages.")

        self.logger.info(f"Communicator {self.name} stopped listening for commands.")
        await self.stop_listening()

    async def get_next_message(self) -> Tuple[PeerIdentity, Message]:
        """Get the next message.

        :raises IndexError: if there is no message to process.
        """
        if len(self._command_queue) == 1:
            self.has_message.clear()
        return self._command_queue.popleft()

    async def send_response(
        self, response: Response[MessageDataType], peer_identity: PeerIdentity
    ):
        """Send response."""
        await self._send_message(response, peer_identity)

    async def send_command(
        self,
        command: Message[MessageDataType],
        peer_identity: PeerIdentity = b"",
        response_timeout: Optional[int] = 600,
        enquire_timeout: int = 1200,
    ) -> Response:
        """Send command and return the response.

        :raises RuntimeError: when response is not received within the
            given timeout.
        """
        command.sequence_number = self.sequence_numbers.setdefault(peer_identity, 1)
        self.sequence_numbers[peer_identity] += 1

        self.logger.debug("Communicator %s: sending command %s.", self.name, command)
        await self._send_message(command, peer_identity)
        response_received_event = EventWithResponse()

        async def enquire(message_uuid):
            """Enquire aboudt message status after enquire_timeout."""
            await asyncio.sleep(enquire_timeout)
            return await self._get_peer_message_status(message_uuid)

        try:
            # Both futures will be canceled in the finally clause.
            enquire_future = asyncio.ensure_future(enquire(command.uuid))
            response_received_future = asyncio.ensure_future(
                response_received_event.wait()
            )

            self._uuid_to_event[command.uuid] = response_received_event
            response = None
            start = time.time()
            done, _ = await asyncio.wait(
                (response_received_future, enquire_future),
                timeout=response_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if response_received_future in done:
                return response_received_event.response

            elif enquire_future in done:
                if enquire_future.result() == MessageStatus.UNKNOWN.value:
                    raise RuntimeError(
                        f"Communicator {self.name}: response not received by peer."
                    )
            else:
                raise RuntimeError(
                    f"Communicator {self.name}: timeout for response to {command} has expired."
                )

            # Enquire future was successfull, peer is probably just processing the message.
            # Wait for response until timeout.
            if response_timeout is None:
                remaining_timeout = None
            else:
                remaining_timeout = max(0, start + response_timeout - time.time())
            try:
                await asyncio.wait_for(
                    response_received_future, timeout=remaining_timeout
                )
            except asyncio.CancelledError:
                raise RuntimeError(
                    f"Communicator {self.name}: timeout for response to {command} has expired."
                )

            response = response_received_event.response
            self.logger.debug("Received response: %s", response)
        finally:
            enquire_future.cancel()
            response_received_future.cancel()

        return response

    async def stop_listening(self):
        """Stop listening.

        Also stop all running commands.

        This method should only be used when not communicator object is not
        used as a context manager.
        """
        self.logger.debug("Communicator %s stopped listening", self.name)
        self._terminating.set()

    async def terminate(self, reason: str):
        """Stop listening and close the socket.

        Also notify the peer about termination.

        :raises AssertionError: when no listening task is running.
        """
        if self._listening_future is not None:
            with suppress(RuntimeError):
                await self.send_command(
                    Message(MessageType.COMMAND, "terminate", reason)
                )
            await self.stop_listening()
            with suppress(asyncio.CancelledError):
                await self._listening_future
        self.writer.close()


class SocketCommunicator(BaseCommunicator):
    """Handles socket communication."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        name: str,
        logger: logging.Logger,
        peer_status_changed: Optional[
            Callable[[PeerIdentity, PeerStatus], Coroutine]
        ] = None,
    ):
        """Initialize."""
        super().__init__(
            name,
            logger,
            reader,
            writer,
            async_send_data,
            async_receive_data,
            peer_status_changed,
        )


class BaseProtocol:
    """Base protocol class."""

    def __init__(
        self,
        communicator: BaseCommunicator,
        logger: logging.Logger,
    ):
        """Initialize."""
        self.communicator = communicator
        self.logger = logger
        self._should_stop = asyncio.Event()

    async def process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Process single command."""
        command_name = received_message.type_data
        handler_name = "handle_" + command_name
        handler = getattr(self, handler_name, None)
        if handler is None:
            handler = self.default_command_handler
        try:
            response = await handler(received_message, peer_identity)
        except Exception as ex:
            self.logger.exception(
                f"Exception while running command handler '{handler_name}'"
            )
            response = received_message.respond_error(
                f"Exception while running command handler {handler_name}: {ex}"
            )

        try:
            await self.communicator.send_response(response, peer_identity)
        except RuntimeError:
            self.logger.exception(
                "Protocol: error sending response to {received_message}."
            )
            await self._abort_with_error("Error sending response.")

    def post_processing_command(
        self,
        peer_identity: PeerIdentity,
        received_message: Message,
        future: asyncio.Future,
    ):
        """Run optional post-processing command."""
        command_name = received_message.type_data
        handler_name = "post_" + command_name
        handler = getattr(self, handler_name, None)
        if handler is not None:
            asyncio.ensure_future(handler(received_message, peer_identity))

    async def default_command_handler(
        self, message: Message, identity: PeerIdentity
    ) -> Response:
        """Handle the response when no other handler is found for the given command."""
        return message.respond(
            f"No handler for command {message.type_data}.", ResponseStatus.ERROR
        )

    async def handle_terminate(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Handle terminate command."""
        return message.respond_ok("OK")

    async def handle_terminating(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Handle peer termination."""
        return message.respond_ok("OK")

    async def communicate(self):
        """Start communication.

        This coroutine stopss when it is canceled or _should_stop flag is set.
        """
        async with self.communicator:
            try:
                while not self._should_stop.is_set():
                    should_stop_future = asyncio.ensure_future(self._should_stop.wait())
                    communicator_terminating_future = asyncio.ensure_future(
                        self.communicator._terminating.wait()
                    )

                    has_message_future = asyncio.ensure_future(
                        self.communicator.has_message.wait()
                    )

                    self.logger.debug(
                        "Communicate %s: waiting for message.", self.communicator.name
                    )
                    done, pending = await asyncio.wait(
                        (
                            should_stop_future,
                            has_message_future,
                            communicator_terminating_future,
                        ),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    self.logger.debug(
                        "Communicate %s got nudged.", self.communicator.name
                    )

                    if has_message_future in done:
                        self.logger.debug(
                            "Communicate %s: got new message.", self.communicator.name
                        )
                        try:
                            (
                                peer_identity,
                                received_command,
                            ) = await self.communicator.get_next_message()
                        except IndexError:
                            # On the next iteration the while loop will stop.
                            self.logger.exception(
                                "Communicator: error receiving next message."
                            )
                            await self._abort_with_error("Error receiving next message")
                        else:
                            # Run process command in the background so the main
                            # process can answer other queries.
                            processing_future = asyncio.ensure_future(
                                self.process_command(peer_identity, received_command)
                            )
                            callback = functools.partial(
                                self.post_processing_command,
                                peer_identity,
                                received_command,
                            )
                            processing_future.add_done_callback(callback)
                    elif communicator_terminating_future in done:
                        # Communicator is closed. Stop.
                        self.logger.debug(
                            "Communicator %s stoped", self.communicator.name
                        )
                        self.stop_communicate()

                    for future in pending:
                        self.logger.debug(
                            "Communicate (%s): cancelling future",
                            self.communicator.name,
                        )
                        future.cancel()
                        self.logger.debug(
                            "Communicate (%s): future cancelled", self.communicator.name
                        )
            except:
                self.logger.exception("Exception while running communicate.")
                await self._abort_with_error("Exception while running communicate.")
        self.logger.debug(
            "Communicator %s stopped communicating", self.communicator.name
        )

    def stop_communicate(self):
        """Stop communicating."""
        self.logger.debug(
            "Protocol using %s: stop communicate set.", self.communicator.name
        )
        self._should_stop.set()

    async def _abort_with_error(self, error: str):
        """Log error and continue by default."""
        self.logger.error(error)
