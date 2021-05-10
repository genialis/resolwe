"""Communication container startup script."""

import array
import asyncio
import functools
import json
import logging
import os
import shutil
import signal
import socket
import sys
import threading
from contextlib import suppress
from distutils.util import strtobool
from pathlib import Path
from typing import Any, Optional, Tuple

import zmq
import zmq.asyncio
from executors import constants, global_settings
from executors.connectors import connectors
from executors.connectors.baseconnector import BaseStorageConnector
from executors.connectors.hasher import StreamHasher
from executors.connectors.transfer import Transfer
from executors.socket_utils import (
    BaseCommunicator,
    BaseProtocol,
    Message,
    PeerIdentity,
    Response,
    ResponseStatus,
    SocketCommunicator,
)
from executors.transfer import transfer_data
from executors.zeromq_utils import ZMQCommunicator

# Socket used to connect with the processing container.
PROCESSING_SOCKET = constants.SOCKETS_VOLUME / constants.COMMUNICATION_PROCESSING_SOCKET
UPLOAD_FILE_SOCKET = constants.SOCKETS_VOLUME / constants.UPLOAD_FILE_SOCKET

# Listener IP and port are read from environment.
LISTENER_IP = os.getenv("LISTENER_SERVICE_HOST", "127.0.0.1")
LISTENER_PORT = os.getenv("LISTENER_SERVICE_PORT", "53893")
LISTENER_PROTOCOL = os.getenv("LISTENER_PROTOCOL", "tcp")
DATA_ID = int(os.getenv("DATA_ID", "-1"))
KEEP_DATA = bool(strtobool(os.environ.get("FLOW_MANAGER_KEEP_DATA", "False")))
RUNNING_IN_KUBERNETES = bool(
    strtobool(os.environ.get("RUNNING_IN_KUBERNETES", "False"))
)

# How many file descriptors to receive over socket in a single message.
DESCRIPTOR_CHUNK_SIZE = int(os.environ.get("DESCRIPTOR_CHUNK_SIZE", 100))

MOUNTED_CONNECTORS = [
    name for name in os.environ["MOUNTED_CONNECTORS"].split(",") if name
]

# Mapping between storage and connectors for this storage.
# The values are tuples: (default connector, default mounted connector)
STORAGE_CONNECTOR: dict[
    str, Tuple[BaseStorageConnector, Optional[BaseStorageConnector]]
] = {}

# Configure container logger. All logs are output to stdout for further
# processing.
# The log level defaults to debug except for boto and google loggers, which
# is set to warning due to them being extremely verbose in the debug mode.
LOG_LEVEL = int(os.getenv("LOG_LEVEL", logging.DEBUG))
BOTO_LOG_LEVEL = int(os.getenv("BOTO_LOG_LEVEL", logging.WARNING))
GOOGLE_LOG_LEVEL = int(os.getenv("GOOGLE_LOG_LEVEL", logging.WARNING))

logging.basicConfig(
    stream=sys.stdout,
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

for boto_logger_name in ["botocore", "boto3", "s3transfer", "urllib3"]:
    logging.getLogger(boto_logger_name).setLevel(BOTO_LOG_LEVEL)

for google_logger in ["google"]:
    logging.getLogger(google_logger).setLevel(GOOGLE_LOG_LEVEL)

logger = logging.getLogger(__name__)

logger.info("Starting communication container for data with id %d.", DATA_ID)


def purge_secrets():
    """Purge the content of the secrets directory.

    The directory itself is mounted as volume, so it can not be deleted.
    """

    def handle_error(func, path, exc_info):
        """Handle permission errors while removing data directories."""
        if isinstance(exc_info[1], PermissionError):
            os.chmod(path, 0o700)
            shutil.rmtree(path)

    try:
        for root, dirs, files in os.walk(constants.SECRETS_VOLUME):
            for f in files:
                os.chmod(os.path.join(root, f), 0o700)
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d), onerror=handle_error)

    except OSError:
        logger.exception("Manager exception while removing data runtime directory.")


class FakeConnector(BaseStorageConnector):
    """Fake connector class.

    Used to send it to the Transfer class.
    """

    def __init__(
        self,
        config: dict,
        name: str,
        file_streams: dict[str, Any],
        hashes: dict[str, str],
    ):
        """Connector initialization."""
        super().__init__(config, name)
        self.path = Path()
        self.supported_hash = ["crc32c", "md5", "awss3etag"]
        self.multipart_chunksize = self.CHUNK_SIZE
        self.file_streams = file_streams
        self.hashes = hashes
        self.get_ensures_data_integrity = True

    def duplicate(self):
        """Return self."""
        return self

    def base_path(self):
        """Get the base path."""
        return self.path

    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        raise NotImplementedError

    def push(self, stream, url):
        """Push data from the stream to the given URL."""
        raise NotImplementedError

    def exists(self, url):
        """Get if the object at the given URL exist."""
        raise NotImplementedError

    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        return self.hashes[os.fspath(url)][hash_type]

    def get_hashes(self, url, hash_types):
        """Get the hashes of the given types for the given object."""
        raise NotImplementedError

    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        raise NotImplementedError

    def get(self, url, stream):
        """Get data from the given URL and write it into the given stream."""
        raise NotImplementedError

    @property
    def can_open_stream(self):
        """Get True if connector can open object as stream."""
        return True

    def presigned_url(
        self,
        url,
        expiration=10,
        force_download=False,
    ):
        """Create a presigned URL."""
        raise NotImplementedError

    def open_stream(self, url, mode):
        """Get stream for data at the given URL."""
        # Skip 'subpath/' in "subpath/...".
        path = Path(url)
        if global_settings.LOCATION_SUBPATH < path:
            url = path.relative_to(global_settings.LOCATION_SUBPATH)
        file_stream = self.file_streams[os.fspath(url)]
        return file_stream


class Uploader(threading.Thread):
    """Upload referenced files to remote location."""

    def __init__(self, manager: "Manager", loop: asyncio.AbstractEventLoop):
        """Initialize."""
        super().__init__()
        self._terminating = False
        self.manager = manager
        self.loop = loop
        self.ready = threading.Event()

    def receive_file_descriptors(
        self, sock: socket.SocketType
    ) -> Tuple[str, dict[str, Any], bool]:
        """Receive file descriptors.

        See https://docs.python.org/3/library/socket.html#socket.socket.recvmsg .

        Protocol:
        1. Size of the filenames array (json) (64bites, #filenames_length).
        2. Connector name, filenames, presigned flag & file descriptors.
        3. Send response in the form {"success": bool, "presigned_urls": list}.
        """
        filenames_length = int.from_bytes(sock.recv(8), byteorder="big")
        logger.debug(
            "Received file descriptors message of length: %d.", filenames_length
        )
        if filenames_length == 0:
            return ("", dict(), False)
        fds = array.array("i")  # Array of ints
        msg, ancdata, flags, addr = sock.recvmsg(
            filenames_length, socket.CMSG_LEN(DESCRIPTOR_CHUNK_SIZE * fds.itemsize)
        )
        logger.debug("Received file descriptors: %s, %s.", msg, ancdata)
        storage_name, filenames, need_presigned_urls = json.loads(msg.decode())
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                # Append data, ignoring any truncated integers at the end.
                fds.frombytes(
                    cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)]
                )
        return (storage_name, dict(zip(filenames, fds)), need_presigned_urls)

    def send_message(self, sock, response):
        """Send message to the socket."""
        payload = json.dumps(response).encode()
        sock.sendall(len(payload).to_bytes(8, byteorder="big"))
        sock.sendall(payload)

    def run(self):
        """Start listening for file descriptors.

        Socket must be constructed here since it is not thread safe.
        """
        server_socket = socket.socket(family=socket.AF_UNIX)
        server_socket.settimeout(constants.CONTAINER_TIMEOUT)
        server_socket.bind(os.fspath(UPLOAD_FILE_SOCKET))
        server_socket.listen()
        self.ready.set()
        # Wait for the connection up to CONTAINER_TIMEOUT seconds.
        # If it fails, the socket will be closed and process terminated if
        # it will try to save some files as outputs.
        try:
            client, info = server_socket.accept()
        except socket.timeout:
            logger.error("Processing container is not connected to the upload socket")
            self.upload_socket.close()
            return

        # Set the timeout for blocking operations to 1 second so we can
        # check for terminating condition.
        client.settimeout(1)
        logger.info("Uploader client connected (%s)." % client)
        with client:
            while not self._terminating:
                try:
                    (
                        storage_name,
                        file_descriptors,
                        need_presigned_urls,
                    ) = self.receive_file_descriptors(client)
                    # Stop if the socket has been closed.
                    if not file_descriptors:
                        break
                except socket.timeout:
                    logger.info("Uploader timeout waiting for data.")
                    continue
                except:
                    logger.exception(
                        "Exception while receiving file descriptors, exiting."
                    )
                    break
                try:
                    presigned_urls = []
                    to_transfer = []
                    logger.debug("Got %s", file_descriptors)
                    hashes: dict[str, str] = dict()
                    referenced_files: dict[str, dict[str, str]] = dict()

                    file_streams = {
                        file_name: os.fdopen(file_descriptor, "rb")
                        for file_name, file_descriptor in file_descriptors.items()
                    }

                    # Get default connector for the given storage name.
                    to_connector = STORAGE_CONNECTOR[storage_name][0]
                    for file_name in file_descriptors:
                        file_descriptor = file_descriptors[file_name]
                        stream = file_streams[file_name]
                        file_size = os.stat(file_descriptor).st_size

                        # Chose chunk size for S3. The chunk_size must be such
                        # that the file_size fits in at most 10_000 chunks. See
                        # https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html
                        # for additional information about this hard limit.

                        # Min chunk size must be 8 mega bytes. This is also the
                        # threshold for multipart uploads.
                        min_chunk_size = 8 * 1024 * 1024
                        needed_chunk_size = int(file_size / 10000) + 1
                        chunk_size = max(min_chunk_size, needed_chunk_size)

                        hasher = StreamHasher(chunk_size=chunk_size)
                        hasher.compute(stream)
                        referenced_files[file_name] = {
                            hash_type: hasher.hexdigest(hash_type)
                            for hash_type in StreamHasher.KNOWN_HASH_TYPES
                        }
                        hashes[
                            os.fspath(global_settings.LOCATION_SUBPATH / file_name)
                        ] = referenced_files[file_name].copy()

                        referenced_files[file_name]["chunk_size"] = chunk_size
                        referenced_files[file_name]["path"] = file_name
                        referenced_files[file_name]["size"] = file_size
                        if need_presigned_urls:
                            presigned_urls.append(
                                to_connector.presigned_url(
                                    global_settings.LOCATION_SUBPATH / file_name,
                                    expiration=7 * 24 * 60 * 60,
                                )
                            )
                        stream.seek(0)

                    to_transfer = list(referenced_files.values())
                    from_connector = FakeConnector(
                        {"path": ""},
                        "File descriptors connector",
                        file_streams,
                        hashes,
                    )
                    transfer = Transfer(from_connector, to_connector)
                    transfer.transfer_objects(
                        global_settings.LOCATION_SUBPATH, to_transfer
                    )
                except:
                    logger.exception("Exception uploading data.")
                    self.send_message(client, {"success": False})
                    break
                else:
                    if to_transfer:
                        future = asyncio.run_coroutine_threadsafe(
                            self.manager.send_referenced_files(to_transfer), self.loop
                        )
                        future_response = future.result()
                        # When data object is in state ERROR all responses will
                        # have error status to indicate processing should
                        # finish ASAP.
                        if (
                            future_response.response_status == ResponseStatus.ERROR
                            and future_response.message_data != "OK"
                        ):
                            response = {"success": False}
                        else:
                            response = {
                                "success": True,
                                "presigned_urls": presigned_urls,
                            }
                    self.send_message(client, response)
                finally:
                    for stream in file_streams.values():
                        stream.close()

    def terminate(self):
        """Stop the uploader thread."""
        self._terminating = True


class ListenerProtocol(BaseProtocol):
    """Listener protocol."""

    def __init__(
        self, communicator: BaseCommunicator, processing_communicator: BaseCommunicator
    ):
        """Initialize."""
        super().__init__(communicator, logger)
        self.processing_communicator = processing_communicator

    async def get_script(self) -> str:
        """Get the script from the listener."""
        response = await self.communicator.send_command(
            Message.command("get_script", "")
        )
        if response.response_status == ResponseStatus.ERROR:
            raise RuntimeError("Response status error while fetching script.")

        return response.message_data

    async def finish(self, return_code: int):
        """Send finish command."""
        await self.communicator.send_command(
            Message.command("finish", {"rc": return_code})
        )

    async def handle_terminate(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Handle terminate command."""
        response = await self.processing_communicator.send_command(
            Message.command("terminate", "")
        )
        response.uuid = message.uuid
        return response


class ProcessingProtocol(BaseProtocol):
    """Processing protocol."""

    def __init__(
        self, communicator: BaseCommunicator, listener_communicator: BaseCommunicator
    ):
        """Initialize."""
        super().__init__(communicator, logger)
        self.listener_communicator = listener_communicator

    async def default_command_handler(
        self, message: Message, identity: PeerIdentity
    ) -> Response:
        """Proxy command to the listener."""
        return await self.listener_communicator.send_command(message, identity)

    async def handle_upload_dirs(
        self, message: Message[list[str]], identity: PeerIdentity
    ) -> Response[str]:
        """Create directories and sent them to the listener.

        This is needed in case empty dirs are referenced.
        """
        subpath = global_settings.LOCATION_SUBPATH
        directories = message.message_data
        referenced_dirs = []
        for directory in directories:
            if storage_connectors := STORAGE_CONNECTOR.get("data"):
                if mounted_connector := storage_connectors[1]:
                    destination_dir = mounted_connector.path / subpath / directory
                    destination_dir.mkdir(parents=True, exist_ok=True)
            referenced_dirs.append({"path": os.path.join(directory, ""), "size": 0})

        return await self.listener_communicator.send_command(
            Message.command("referenced_files", referenced_dirs)
        )

    async def process_script(self, script: str) -> int:
        """Send the script to the processing container.

        This method can be very long running as it waits for the return code
        the processing container.

        :returns: return code of the process running the script.
        """
        try:
            response = await self.communicator.send_command(
                Message.command("process_script", script), response_timeout=None
            )
            return response.message_data
        except asyncio.CancelledError:
            return 1

    async def terminate(self):
        """Terminate the processing container."""
        await self.communicator.send_command(Message.command("terminate", ""))


class Manager:
    """Main class.

    Communicate with the listener and with the processing container.
    """

    def __init__(self):
        """Initialize."""
        self.processing_communicator: Optional[BaseCommunicator] = None
        self.listener_communicator: Optional[BaseCommunicator] = None
        self.processing_container_connected = asyncio.Event()
        self._process_script_task: Optional[asyncio.Task] = None

    async def send_referenced_files(self, referenced_files):
        """Send referenced files to the listener."""
        return await self.listener_communicator.send_command(
            Message.command("referenced_files", referenced_files)
        )

    async def _handle_processing_container_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle incoming connection from the processing container.

        Python process starts a single connection while Resolwe runtime utils
        starts a new connection for every request.
        """
        logger.debug("Processing container connected")

        # Notify the processing container that the connection is ready.
        writer.write(b"PING\n")
        await writer.drain()
        self.processing_container_connected.set()
        self.processing_communicator = SocketCommunicator(
            reader, writer, "(self <-> processing)", logger
        )

    async def start_processing_socket(self):
        """Start listening on the processing file socket.

        The socket is used by the processing container to communicatite with
        us.
        """
        await asyncio.start_unix_server(
            self._handle_processing_container_connection, os.fspath(PROCESSING_SOCKET)
        )
        logger.debug("Started listening on %s.", PROCESSING_SOCKET)

    async def open_listener_connection(self) -> ZMQCommunicator:
        """Connect to the listener service.

        We are using data id as identity. This implies only one process per
        data object at any given point in time can be running.
        """
        zmq_context = zmq.asyncio.Context.instance()
        zmq_socket = zmq_context.socket(zmq.DEALER)
        zmq_socket.setsockopt(zmq.IDENTITY, str(DATA_ID).encode())
        connect_string = f"{LISTENER_PROTOCOL}://{LISTENER_IP}:{LISTENER_PORT}"
        logger.debug("Opening listener connection to %s", connect_string)
        zmq_socket.connect(connect_string)
        return ZMQCommunicator(zmq_socket, "worker <-> listener", logger)

    async def transfer_missing_data(self):
        """Transfer missing data.

        Log error re-raise exception on failure.

        :raises: RuntimeError on failure.
        """
        try:

            await transfer_data(self.listener_communicator)
        except RuntimeError:
            with suppress(Exception):
                await self.listener_communicator.send_command(
                    Message.command(
                        "process_log", {"error": ["Error transfering missing data."]}
                    )
                )
            raise

    def _communicator_stopped(self, future: asyncio.Future):
        """Stop processing if necessary."""
        if self._process_script_task:
            logger.debug("Communicator closed, cancelling script processing.")
            self._process_script_task.cancel()

    async def start(self) -> int:
        """Start the main program."""
        try:
            return_code = 1
            logger.debug("Starting upload thread")
            upload_thread = Uploader(self, asyncio.get_running_loop())
            upload_thread.start()
            # Wait up to 60 seconds for uploader to get ready.
            if not upload_thread.ready.wait(60):
                logger.error("Upload thread failed to start, terminating.")
                raise RuntimeError("Upload thread failed to start.")

            await self.start_processing_socket()
            self.listener_communicator = await self.open_listener_connection()
            try:
                logger.debug("Waiting for the processing container to connect")
                await asyncio.wait_for(
                    self.processing_container_connected.wait(),
                    constants.CONTAINER_TIMEOUT,
                )
            except asyncio.TimeoutError:
                message = "Unable to connect to the processing container."
                logger.critical(message)
                with suppress(Exception):
                    await self.listener_communicator.send_command(
                        Message.command("process_log", {"error": [message]})
                    )
                sys.exit(1)

            logger.debug("Connected to the processing container.")

            listener = ListenerProtocol(
                self.listener_communicator, self.processing_communicator
            )
            processing = ProcessingProtocol(
                self.processing_communicator, self.listener_communicator
            )

            try:
                # Start listening for messages from the communication and the
                # processing container.
                listener_task = asyncio.ensure_future(listener.communicate())

                # Initialize settings constants by bootstraping.
                response = await self.listener_communicator.send_command(
                    Message.command("bootstrap", (DATA_ID, "communication"))
                )
                global_settings.initialize_constants(DATA_ID, response.message_data)
                modify_connector_settings()
                # Recreate connectors with received settings.
                connectors.recreate_connectors()
                set_default_storage_connectors()

                processing_task = asyncio.ensure_future(processing.communicate())
                listener_task.add_done_callback(self._communicator_stopped)
                processing_task.add_done_callback(self._communicator_stopped)

                await self.listener_communicator.send_command(
                    Message.command("update_status", "PR")
                )

                script = await listener.get_script()
                self._process_script_task = asyncio.create_task(
                    processing.process_script(script)
                )
                return_code = await self._process_script_task
                self._process_script_task = None

            except RuntimeError as runtime_exception:
                logger.exception("Error processing script.")
                with suppress(Exception):
                    await self.listener_communicator.send_command(
                        Message.command(
                            "process_log",
                            {
                                "error": [
                                    "Runtime error in communication container: "
                                    f"{runtime_exception}."
                                ]
                            },
                        )
                    )

        except Exception:
            logger.exception("While running communication container")

        finally:
            logger.debug("Terminating upload thread.")
            upload_thread.terminate()
            upload_thread.join()
            if not KEEP_DATA:
                purge_secrets()

            # Notify listener that the processing is finished.
            try:
                await listener.finish(return_code)
            except RuntimeError:
                logger.exception("Error sending finish command.")
            except:
                logger.exception("Unknown error sending finish command.")

            listener.stop_communicate()
            processing.stop_communicate()

            # Wait for up to 10 seconds to close the tasks.
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(listener_task, processing_task), timeout=10
                )
            return return_code


def set_default_storage_connectors():
    """Set default mounted connector for each known storage."""
    storages = global_settings.SETTINGS["FLOW_STORAGE"]
    for storage_name, storage_settings in storages.items():
        storage_connectors = connectors.for_storage(storage_name)
        default_connector = storage_connectors[0]
        default_mounted_connector = None
        for connector in storage_connectors:
            if connector.name in MOUNTED_CONNECTORS:
                default_mounted_connector = connector
                break

        STORAGE_CONNECTOR[storage_name] = (default_connector, default_mounted_connector)


def modify_connector_settings():
    """Modify mountpoints and add processing and input connectors.

    The path settings on filesystem connectors point to the path on the worker
    node. They have to be remaped to a path inside container and processing and
    input connector settings must be added.
    """
    connector_settings = global_settings.SETTINGS["STORAGE_CONNECTORS"]
    storages = global_settings.SETTINGS["FLOW_STORAGE"]
    connector_storage = {
        connector_name: storage_name
        for storage_name, storage_settings in storages.items()
        for connector_name in storage_settings["connectors"]
    }

    # Point connector path to the correct mountpoint.
    for connector_name in MOUNTED_CONNECTORS:
        storage_name = connector_storage[connector_name]
        connector_settings[connector_name]["config"]["path"] = Path(
            f"/{storage_name}_{connector_name}"
        )


def sig_term_handler(manager_task: asyncio.Task):
    """Gracefully terminate the running process."""
    logger.debug("SIG_INT received, shutting down.")
    manager_task.cancel()


async def start_communication_container():
    """Start the processing manager and set SIGINT handler."""
    manager = Manager()
    manager_task = asyncio.create_task(manager.start())

    asyncio.get_event_loop().add_signal_handler(
        signal.SIGINT, functools.partial(sig_term_handler, manager_task)
    )

    # Wait for the manager task to finish.
    return await manager_task


if __name__ == "__main__":
    sys.exit(asyncio.run(start_communication_container()))
