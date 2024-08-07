"""Utility methods for receiving/sending data over socket."""

import asyncio
import functools
import json
import logging
import socket
import time
import uuid
from collections import defaultdict, deque
from contextlib import suppress
from enum import Enum, unique
from time import time as now
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

logger = logging.getLogger(__name__)


PeerIdentity = bytes
MessageDataType = TypeVar("MessageDataType")
ResponseDataType = TypeVar("ResponseDataType")


class MessageProcessingEventType(Enum):
    """Type of the event in the message processing pipeline."""

    MESSAGE_RECEIVED = "MR"
    MESSAGE_PROCESSING_STARTED = "MPS"
    MESSAGE_PROCESSING_FINISHED = "MPF"
    PREPARATION_FINISHED = "PF"


class MessageProcessingCallback:
    """Event in the message processing pipeline."""

    def event(
        self,
        event_type: MessageProcessingEventType,
        message: "Message",
        peer_identity: PeerIdentity,
    ):
        """Event handler."""
        raise NotImplementedError("Method must be implemented in the subclass.")


@unique
class MessageType(Enum):
    """Type of the message."""

    COMMAND = "COMMAND"
    RESPONSE = "RESPONSE"
    HEARTBEAT = "HBT"


class ResponseStatus(Enum):
    """Response status."""

    OK = "OK"
    ERROR = "ERR"
    SKIP = "SKIP"


class PeerStatus(Enum):
    """Peer status."""

    RESPONSIVE = "responsive"
    DEGRADED = "degraded"
    UNRESPONSIVE = "unresponsive"


class ReceiveStatus(Enum):
    """Status of the received method."""

    OK = "OK"
    INVALID_MESSAGE = "invalid"
    CANCELLED = "cancelled"
    SOCKET_CLOSED = "socket_closed"
    TERMINATED = "terminated"


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
                    sleep = min(max_sleep, min_sleep * (2**retry))
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
                    sleep = min(max_sleep, min_sleep * (2**retry))
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
    writer.write(message_size + message)
    await writer.drain()


async def async_receive_data(
    reader: asyncio.StreamReader, size_bytes: int = 8
) -> Tuple[PeerIdentity, Any, Optional[bytes]]:
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
    return (b"", json.loads(received.decode("utf-8")), None)


class Message(Generic[MessageDataType]):
    """Base class for message."""

    def __init__(
        self,
        message_type: MessageType,
        type_data: str,
        message_data: MessageDataType,
        message_uuid: Optional[str] = None,
        sent_timestamp: Optional[float] = None,
        client_id: Optional[bytes] = None,
    ):
        """Initialize.

        The sent_timestamp is auto-set on sending message to timestamp.
        """
        self.message_type = message_type
        self.message_data = message_data
        self.type_data = type_data
        self.uuid = message_uuid or self._get_random_message_identifier()
        self.sent_timestamp = sent_timestamp
        self.client_id = client_id

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
        """Get command name.

        :raises AssertionError: if message type is not ``MessageType.COMMAND``.
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
        )

    def respond_skip(
        self, response_data: ResponseDataType
    ) -> "Response[ResponseDataType]":
        """Set the response status to skip."""
        return self.respond(response_data, ResponseStatus.SKIP)

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
        client_id: Optional[bytes] = None,
    ) -> "Message[MessageDataType]":
        """Construct and return a command."""
        return Message(
            MessageType.COMMAND,
            command_name,
            message_data,
            message_uuid,
            client_id=client_id,
        )

    @staticmethod
    def heartbeat() -> "Message[str]":
        """Construct the heartbeat message."""
        return Message(MessageType.HEARTBEAT, "", "")

    @staticmethod
    def is_valid(message_dict: Dict) -> bool:
        """Validate the dictionary representing the message."""
        try:
            required_keys = ("type", "type_data", "data")
            optional_keys = ("uuid",)
            required_value_types = (str, str, object)
            optional_value_types = (str,)
            assert set(required_keys).issubset(set(message_dict.keys()))
            assert all(
                isinstance(message_dict[key], value_type)
                for key, value_type in zip(required_keys, required_value_types)
            )
            assert all(
                isinstance(message_dict[key], value_type)
                for key, value_type in zip(optional_keys, optional_value_types)
                if key in message_dict
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
            message_dict.get("timestamp"),
            message_dict.get("client_id"),
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
            "timestamp": self.sent_timestamp,
        }

    def time_elapsed(self) -> Optional[float]:
        """Get the time elapsed from when message was sent until now.

        :return: the float representing number of seconds passed from the
            moment the message was sent or None if it can not be determined.
        """
        if self.sent_timestamp is None:
            return None
        return now() - self.sent_timestamp

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
        sent_timestamp: Optional[float] = None,
    ):
        """Initialize."""
        super().__init__(
            MessageType.RESPONSE,
            type_data,
            message_data,
            message_uuid,
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
            message_dict.get("timestamp"),
        )


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

    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        reader: Any,
        writer: Any,
        send_method: Callable,
        receive_method: Callable,
        heartbeat_handler: Optional[Callable[[PeerIdentity], Any]] = None,
    ):
        """Initialize.

        :param name: used in logging to identify the communicator.
        :param logger: logger to use for logging.
        :param reader: object to use when receiving messages.
        :param writer: object to use when sending messages.
        :param send_method: method to use when sending messages.
        :param receive_method: method to use when receiving messages.
        :param heartbeat_handler: method to call on heartbeat messages.
        """
        self.name = name
        self.logger = logger
        self._terminating = asyncio.Event()
        self.has_message = asyncio.Event()
        self.send_method = send_method
        self.receive_method = receive_method
        self.reader = reader
        self.writer = writer
        self.heartbeat_handler = heartbeat_handler

        self._listening_future: Optional[asyncio.Future] = None
        # Wait for x seconds for listening future to complete when _terminating
        # flag is set.
        self._listening_future_wait_timeout = 30

        self._command_queue: Deque[Tuple[PeerIdentity, Message]] = deque()

        self._uuid_to_event: Dict[str, EventWithResponse] = dict()

        # Keep last uuid and timestamp from every peer to avoid forwarding
        # duplicated requests.
        self._uuids_received: Dict[PeerIdentity, Dict[str, int]] = defaultdict(dict)

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

    async def _receive_message(
        self,
    ) -> Tuple[ReceiveStatus, Optional[Tuple[PeerIdentity, Message]]]:
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
        received_status = ReceiveStatus.INVALID_MESSAGE
        try:
            self.logger.debug("Communicator %s waiting for message.", self.name)
            receive_task = asyncio.ensure_future(self.receive_method(self.reader))
            terminating_task = asyncio.ensure_future(self._terminating.wait())
            done, pending = await asyncio.wait(
                (receive_task, terminating_task), return_when=asyncio.FIRST_COMPLETED
            )
            if receive_task in done:
                try:
                    received = receive_task.result()
                except asyncio.IncompleteReadError:
                    received_status = ReceiveStatus.SOCKET_CLOSED
                    self.logger.info("Socket closed by peer, stopping communication.")
                    received = None
                if received is not None:
                    assert isinstance(received, tuple)
                    assert len(received) == 3
                    assert isinstance(received[0], bytes)
                    received[1]["client_id"] = received[2]
                    assert Message.is_valid(received[1])
                    result = received[0], Message.from_dict(received[1])
                    received_status = ReceiveStatus.OK
            else:
                received_status = ReceiveStatus.TERMINATED
                self.logger.debug(
                    "Communicator %s _receive_message: terminating flag is set, returning None",
                    self.name,
                )
        except asyncio.CancelledError:
            received_status = ReceiveStatus.CANCELLED
            self.logger.debug(
                "Communicator %s: CancelledError in _receive_message.", self.name
            )
        except:
            received_status = ReceiveStatus.INVALID_MESSAGE
            self.logger.exception(
                "Communicator %s: exception in _receive_message.", self.name
            )
        finally:
            # Always stop both tasks.
            receive_task.cancel()
            terminating_task.cancel()
        return received_status, result

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

        :raises RuntimeError: if sending method raises exception or timeout
            occurs sending message.
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
            except BrokenPipeError:
                # No need to retry, the pipe will not reconnect.
                error_message = f"Communicator {self.name}: broaken pipe."
                self.logger.exception(error_message)
                raise RuntimeError(error_message)

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
        self._terminating.clear()
        self._listening_future = asyncio.ensure_future(self.start_listening())
        self.logger.debug("Communicator %s: entering context.", self.name)
        return self

    async def __aexit__(self, typ, value, trace):
        """On exiting a context, stop listening for messages."""
        self.logger.debug(f"Communicator {self.name}: leaving context.")
        assert self._listening_future is not None

        self._terminating.set()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._listening_future, timeout=self._listening_future_wait_timeout
            )
        self._listening_future = None
        self.logger.debug("Communicator %s: leaving context.", self.name)

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
                status, received = await self._receive_message()

                if received is None:
                    if status in [
                        ReceiveStatus.SOCKET_CLOSED,
                        ReceiveStatus.TERMINATED,
                        ReceiveStatus.CANCELLED,
                    ]:
                        break
                    elif ReceiveStatus.INVALID_MESSAGE:
                        continue

                identity, message = received

                if message.message_type is MessageType.HEARTBEAT:
                    logger.debug("Got heartbeat from peer '%s'.", identity)
                    if self.heartbeat_handler is not None:
                        asyncio.ensure_future(self.heartbeat_handler(identity))

                elif message.message_type is MessageType.COMMAND:
                    if message.uuid not in self._uuids_received[identity]:
                        self._command_queue.append((identity, message))
                        self.logger.debug(
                            "Received command '%s' from peer '%s'.",
                            message.command_name,
                            identity,
                        )
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
                self._uuids_received[identity] = {message.uuid: now()}

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

    async def send_heartbeat(self, peer_identity: PeerIdentity = b""):
        """Send the heartbeat message to the peer."""
        with suppress(RuntimeError):
            await self.send_command(Message.heartbeat(), timeout=1)

    async def send_command(
        self,
        command: Message[MessageDataType],
        peer_identity: PeerIdentity = b"",
        resend_timeout: Optional[int] = 60,
        timeout: Optional[int] = 1200,
    ) -> Response:
        """Send the command and return the response.

        In order to receive the response, the listening task must be running.

        :attr resend_timeout: retry sending the message after resend_timeout
            elapsed and no response is received. The message can be resent
            multiple times, each time the timeout between two sending events
            is doubled.
            When None is given no retry is performed.

        :attr timeout: abort waiting for response after timeout. When None wait
            forever.

        :attr await_response: when set to true return imediatelly and do not
            wait for the response.

        :raises RuntimeError: when response is not received within the
            given timeout.
        """
        self.logger.debug(
            "Communicator %s: sending message '%s' to peer '%s'.",
            self.name,
            command.type_data,
            peer_identity,
        )
        await self._send_message(command, peer_identity)
        response_received_event = EventWithResponse()
        timeout_task: Optional[asyncio.Task] = None
        resend_task: Optional[asyncio.Task] = None
        base_tasks: List[asyncio.Task] = []
        self._uuid_to_event[command.uuid] = response_received_event
        response_received_task = asyncio.ensure_future(response_received_event.wait())
        base_tasks.append(response_received_task)
        if timeout:
            timeout_task = asyncio.ensure_future(asyncio.sleep(timeout))
            base_tasks.append(timeout_task)

        try:
            while True:
                tasks_to_wait = base_tasks[:]
                if resend_timeout:
                    resend_task = asyncio.ensure_future(asyncio.sleep(resend_timeout))
                    resend_timeout *= 2
                    tasks_to_wait.append(resend_task)

                done, _ = await asyncio.wait(
                    tasks_to_wait, return_when=asyncio.FIRST_COMPLETED
                )

                # Message received.
                if response_received_task in done:
                    self.logger.debug(
                        "Received response to command '%s' with uuid '%s'.",
                        command.command_name,
                        command.uuid,
                    )
                    return response_received_event.response
                # Timeout.
                elif timeout_task in done:
                    raise RuntimeError(
                        f"Communicator {self.name}: no response to command "
                        f"{command.message_type} with uuid {command.uuid} "
                        f"from peer {peer_identity} in '{timeout} seconds'."
                    )
                # Resend task
                elif resend_task in done:
                    await self._send_message(command, peer_identity)
        finally:
            if resend_task:
                resend_task.cancel()
            if timeout_task:
                timeout_task.cancel()
            response_received_task.cancel()

    async def stop_listening(self):
        """Stop listening.

        Also stop all running commands.

        This method should only be used when not communicator object is not
        used as a context manager.
        """
        self.logger.debug("Stopping communicator %s.", self.name)
        self._terminating.set()

    async def terminate(self, reason: str):
        """Stop listening and close the socket.

        :raises AssertionError: when no listening task is running.
        """
        if self._listening_future is not None:
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
        heartbeat_handler: Optional[Callable[[PeerIdentity], Any]] = None,
    ):
        """Initialize."""
        super().__init__(
            name,
            logger,
            reader,
            writer,
            async_send_data,
            async_receive_data,
            heartbeat_handler,
        )


class BaseProtocol:
    """Base protocol class."""

    def __init__(
        self,
        communicator: BaseCommunicator,
        logger: logging.Logger,
        max_concurrent_commands: int = 10,
        event_callback: Optional[MessageProcessingCallback] = None,
    ):
        """Initialize."""
        self.communicator = communicator
        self.logger = logger
        self._should_stop = asyncio.Event()
        self._max_concurrent_commands = max_concurrent_commands
        self._concurrent_semaphore = asyncio.Semaphore(self._max_concurrent_commands)
        self._event_callback = event_callback

    def _call_event(
        self,
        event_type: MessageProcessingEventType,
        message: Message,
        peer_id: PeerIdentity,
    ):
        """Call the event callback."""
        if self._event_callback is not None:
            self._event_callback.event(event_type, message, peer_id)

    async def process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Process single command.

        Use semaphore to make sure no more than max_concurrent_commands are
        processed at any given time.
        """
        async with self._concurrent_semaphore:
            command_name = received_message.type_data
            self._call_event(
                MessageProcessingEventType.MESSAGE_PROCESSING_STARTED,
                received_message,
                peer_identity,
            )
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

            # Do not send response when status is set to SKIP.
            if response.status != ResponseStatus.SKIP:
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
            self.logger.debug("Running post processing handler '%s'.", handler_name)
            asyncio.ensure_future(handler(received_message, peer_identity))
        self._call_event(
            MessageProcessingEventType.MESSAGE_PROCESSING_FINISHED,
            received_message,
            peer_identity,
        )

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
        self._should_stop.clear()
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

                    if has_message_future in done:
                        self.logger.debug(
                            "Communicate %s: got new message.", self.communicator.name
                        )
                        try:
                            (
                                peer_identity,
                                received_command,
                            ) = await self.communicator.get_next_message()
                            self._call_event(
                                MessageProcessingEventType.MESSAGE_RECEIVED,
                                received_command,
                                peer_identity,
                            )
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
