"""Processing container startup script."""

import array
import asyncio
import concurrent
import json
import logging
import os
import select
import shlex
import shutil
import signal
import socket
import sys
import uuid
from contextlib import closing, suppress
from distutils.util import strtobool
from functools import partial
from pathlib import Path
from time import time
from typing import Dict, List, Optional, Sequence

try:
    import constants
    from socket_utils import (
        BaseCommunicator,
        BaseProtocol,
        Message,
        PeerIdentity,
        Response,
        SocketCommunicator,
    )

except ImportError:
    from resolwe.flow.executors import constants
    from resolwe.flow.executors.socket_utils import (
        BaseCommunicator,
        BaseProtocol,
        Message,
        PeerIdentity,
        Response,
        SocketCommunicator,
    )

# The directory where local processing is done.
TMP_DIR = Path(os.environ.get("TMP_DIR", ".tmp"))
DATA_ID = int(os.getenv("DATA_ID", "-1"))

# Basic constants.
WORKING_DIRECTORY = Path.cwd().resolve()
STDOUT_LOG_PATH = WORKING_DIRECTORY / "stdout.txt"
JSON_LOG_PATH = WORKING_DIRECTORY / "jsonout.txt"

# Update log files every 30 seconds.
UPDATE_LOG_FILES_TIMEOUT = 30

# Keep data settings.
KEEP_DATA = bool(strtobool(os.environ.get("FLOW_MANAGER_KEEP_DATA", "False")))

# Read configuration from environmental variables.
COMMUNICATION_SOCKET = (
    constants.SOCKETS_VOLUME / constants.COMMUNICATION_PROCESSING_SOCKET
)
SCRIPT_SOCKET = constants.SOCKETS_VOLUME / constants.SCRIPT_SOCKET
UPLOAD_FILE_SOCKET = constants.SOCKETS_VOLUME / constants.UPLOAD_FILE_SOCKET

# How many file descriptors to sent over socket in a single message.
DESCRIPTOR_CHUNK_SIZE = int(os.environ.get("DESCRIPTOR_CHUNK_SIZE", 100))

# Configure container log level.
LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.DEBUG))

# Max characters to log in a single message: the processing script messages can
# be hundreds lines long.
LOG_MAX_LENGTH = int(os.environ.get("LOG_MAX_LENGTH", 200))

# How long to wait for the response from the communication container.
# Do not set too low or script may fail to start. The default is 20 minutes.
SCRIPT_START_TIMEOUT = int(os.environ.get("SCRIPT_START_TIMEOUT", 1200))

logging.basicConfig(
    stream=sys.stdout,
    level=LOG_LEVEL,
    format=f"%(asctime)s - %(name)s - %(levelname)s - %(message).{LOG_MAX_LENGTH}s",
)
logger = logging.getLogger(__name__)


assert sys.version_info[0] == 3, "Only Python3 is supported"
assert sys.version_info[1] >= 6, "Only Python >= 3.6 is supported"


async def stop_task(task: Optional[asyncio.Task], timeout: int = 1):
    """Cancel the given task.

    Also wait for task to stop for up to timeout seconds.
    """
    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=timeout)


async def log_exception(
    message: str, communicator: BaseCommunicator, timeout: int = 60
):
    """Log the exception via logger and send it to the communicator.

    Log it using logger and send error message to the communication
    container. Make sure no exception is ever raised and that the method
    completes in timeout seconds.
    """
    with suppress(Exception):
        logger.exception(message)
    with suppress(Exception):
        log_coroutine = communicator.process_log({"error": [message]})

        await asyncio.wait_for(log_coroutine, timeout)


async def heartbeat_task(communicator: BaseCommunicator, heartbeat_interval: int = 60):
    """Periodically send heartbeats.

    This task will run indefinitely and has to be cancelled to stop.

    :attr heartbeat_interval: seconds between successive heartbeat messages.
    """
    while True:
        await communicator.send_heartbeat()
        await asyncio.sleep(heartbeat_interval)


async def log_error(message: str, communicator: BaseCommunicator, timeout: int = 60):
    """Log the error via logger and send it to the communicator.

    Log it using logger and send error message to the communication
    container. Make sure no exception is ever raised and that the method
    completes in timeout seconds.
    """
    with suppress(Exception):
        logger.error(message)
    with suppress(Exception):
        log_coroutine = communicator.process_log({"error": [message]})

        await asyncio.wait_for(log_coroutine, timeout)


async def connect_to_communication_container(path: str) -> SocketCommunicator:
    """Wait for the communication container to start.

    Assumption is that the communication container is accepting connections
    and will send a single PING when connected.

    The coroutine will only exit when the connection is successfully
    established it is the responsibility of the caller to terminate the
    coroutine when the connection can not be established for some time.
    """
    while True:
        with suppress(Exception):
            reader, writer = await asyncio.open_unix_connection(path)
            line = (await reader.readline()).decode("utf-8")
            assert line.strip() == "PING", "Expected 'PING', got '{}'.".format(line)
            return SocketCommunicator(
                reader, writer, "processing<->communication", logger
            )
        await asyncio.sleep(1)


async def start_script(script: str) -> asyncio.subprocess.Process:
    """Start processing the script.

    :raises Exception: when script could not be started.
    """
    # Start the bash subprocess and write script to its standard input.
    logger.debug("Executing script: '%s'.", script)
    bash_command = "/bin/bash --login" + os.linesep
    proc = await asyncio.subprocess.create_subprocess_exec(
        *shlex.split(bash_command),
        limit=4 * (2**20),  # 4MB buffer size for line buffering
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    # Assert that stdin and stdout streams are connected to the proc.
    error_message = "The stream '{}' could not connect to the script."
    assert proc.stdin is not None, error_message.format("stdin")
    assert proc.stdout is not None, error_message.format("stdout")
    proc.stdin.write(script.encode("utf-8"))
    proc.stdin.close()
    logger.debug("Code sent to stdin of the script process.")
    return proc


async def communicate_with_process(
    proc: asyncio.subprocess.Process, manager: "ProcessingManager"
) -> int:
    """Read the given process standard output and write it to the log file.

    :return: the process's return code.
    """
    try:
        assert proc.stdout is not None
        while not proc.stdout.at_eof():
            # Here we tap into the internal structures of the StreamReader
            # in order to read as much data as we can in one chunk.
            await proc.stdout._wait_for_data("execute_script")  # type: ignore
            # Send received data to upload manager.
            manager.upload_handler.update_stdout(proc.stdout._buffer)
            proc.stdout._buffer.clear()
            # Break the loop if EOF is reached.
            if proc.stdout.at_eof():
                logger.debug("Processing script stdout eof.")
                break
        # Wait for the script to terminate.
        await proc.wait()

    except Exception:
        logger.exception("Exception running the script process.")
        # Terminate process if it is still running.
        with suppress(Exception):
            if proc.returncode is None:
                proc.terminate()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(proc.wait(), timeout=60)
            if proc.returncode is None:
                proc.kill()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(proc.wait(), timeout=60)
    finally:
        # Return the return code (if available) or general error code 1.
        return_code = proc.returncode if proc.returncode is not None else 1
        logger.debug("Script finished with the return code '%d'.", return_code)
        manager.terminate()
        return return_code


async def initialize_connections(
    loop: asyncio.AbstractEventLoop,
) -> "ProcessingManager":
    """Start the processing manager.

    This method does the following:
    - connects to the communication container,
    - connects to the upload socket (unix socket) and
    - starts the socket server for the communication with the processing
        script.

    :raises ConnectionError: when connection can not be established.
    :return: the processing manager instance.
    """
    # Connect to the communication container and start processing script
    # server first.
    logger.debug(
        "Connecting to the communication container via socket '%s'.",
        COMMUNICATION_SOCKET,
    )
    start = time()
    communicator_future: asyncio.Task = asyncio.ensure_future(
        connect_to_communication_container(str(COMMUNICATION_SOCKET))
    )
    try:
        socket_communicator = await asyncio.wait_for(
            communicator_future, timeout=constants.CONTAINER_TIMEOUT
        )
    except (asyncio.CancelledError, asyncio.TimeoutError):
        raise ConnectionError("Timeout connecting to communication container.")
    logger.debug(
        "Connected to the communication container in '%.2f' seconds.",
        time() - start,
    )

    # Connect to the upload socket and start the periodic upload of log files.
    upload_socket = socket.socket(family=socket.AF_UNIX)
    upload_socket.connect(str(UPLOAD_FILE_SOCKET))
    upload_handler = UploadHandler(upload_socket, loop)
    logger.debug("Connected to the upload socket '%s'.", UPLOAD_FILE_SOCKET)
    upload_handler.start_log_upload()

    # Start the server to handle processing script connections.
    logger.debug(
        "Starting the processing script server on the socket '%s'.", SCRIPT_SOCKET
    )
    script_server_future: asyncio.Task = asyncio.ensure_future(
        asyncio.start_unix_server(
            partial(
                handle_processing_script_connection,
                socket_communicator,
                upload_handler,
            ),
            str(SCRIPT_SOCKET),
        )
    )
    try:
        script_server = await asyncio.wait_for(script_server_future, timeout=60)
    except asyncio.CancelledError:
        raise ConnectionError("Timeout starting processing script server.")

    return ProcessingManager(socket_communicator, upload_handler, script_server, loop)


async def handle_processing_script_connection(
    communication_communicator: SocketCommunicator,
    upload_handler: "UploadHandler",
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
):
    """Handle incoming connection from the processing script.

    Python process opens a single connection while Resolwe runtime utils opens
    a new connection for every request.
    """
    try:
        logger.info("Processing script connected to the socket.")
        communicator = SocketCommunicator(reader, writer, "script", logger)
        protocol_handler = ScriptProtocol(
            communication_communicator, communicator, upload_handler
        )
        await protocol_handler.communicate()
    except (asyncio.IncompleteReadError, GeneratorExit):
        logger.info("Processing script closed connection.")
    except Exception:
        logger.exception("Unknown exception while handling processing script message.")
        raise


class CommunicationProtocol(BaseProtocol):
    """Handles communication with the communication container."""

    def __init__(self, manager: "ProcessingManager"):
        """Initialize variables."""
        super().__init__(manager.communicator, logger)
        self.processing_manager = manager
        self.script_task: Optional[asyncio.Task] = None
        self._script_starting = asyncio.Lock()
        self.script_started = asyncio.Event()

    async def handle_process_script(
        self, message: Message, identity: PeerIdentity
    ) -> Response[int]:
        """Handle the process_script message."""
        # This message can arrive multiple times so make sure the script is
        # started only once.
        if self.script_started.is_set():
            return message.respond_ok("Already processing")
        self.script_started.set()
        if self.script_task is None:
            process = await start_script(message.message_data)
            self.script_task = asyncio.ensure_future(
                communicate_with_process(process, self.processing_manager)
            )
        return message.respond_ok("Script started.")

    async def terminate_script(self):
        """Terminate the script."""
        await stop_task(self.script_task)

    async def post_terminate(self, message: Message, identity: PeerIdentity):
        """Stop the processing script and stop the manager."""
        logger.debug("Stopping all the tasks in the post_terminate handler.")
        await stop_task(self.script_task)
        self.processing_manager.terminate()

    async def handle_terminate(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Handle the terminate command."""
        logger.debug("Received terminate command from the communication container.")
        return message.respond_ok("Terminating")


class ProcessingManager:
    """Manager class.

    It starts network servers and executes the received script.
    """

    def __init__(
        self,
        communicator: BaseCommunicator,
        upload_handler: "UploadHandler",
        script_server,
        loop: asyncio.AbstractEventLoop,
    ):
        """Initialize variables, start the communicator and heartbeat task."""
        self.upload_handler = upload_handler
        self.communicator = communicator
        self.loop = loop
        self.protocol_handler = CommunicationProtocol(self)
        self.script_server = script_server

        self.communicator_task = asyncio.ensure_future(
            self.protocol_handler.communicate()
        )
        self.heartbeat_task = asyncio.ensure_future(heartbeat_task(communicator))

        # TODO: drop this when we stop supporting Python 3.6.
        # This class has been implicitly getting the current running loop since 3.7.
        lock_arguments: Dict[str, asyncio.AbstractEventLoop] = {}
        if sys.version_info < (3, 7):
            lock_arguments = {"loop": loop}
        self._terminating = asyncio.Event(**lock_arguments)

    def terminate(self):
        """Terminate the processing container."""
        self._terminating.set()

    async def _stop_all_tasks(self, stop_timeout=60):
        """Stop all running tasks."""
        # Stop the processing script.
        logger.debug("Stopping all the tasks in the processing manager.")
        logger.debug("Stopping the processing script task.")
        await stop_task(self.protocol_handler.script_task)

        # Stop the script server.
        logger.debug("Stopping the script server.")
        with suppress(Exception):
            self.script_server.close()
            await asyncio.wait_for(
                self.script_server.wait_closed(), timeout=stop_timeout
            )

        # Stop the periodic log upload task.
        logger.debug("Stopping periodic log upload task.")
        await self.upload_handler.stop_log_upload()

        # Upload latest log files (in case there was a change). Notify the
        # listener on failure.
        logger.debug("Uploading latest log files.")
        try:
            # Give it up to one minute to upload the latest log files.
            latest_log_upload = asyncio.ensure_future(
                self.upload_handler.upload_log_files()
            )
            await asyncio.wait_for(latest_log_upload, timeout=60)
        except Exception:
            await log_exception("Unable to upload the latest logs.", self.communicator)

        # If data should be kept it must be uploaded to the data dir.
        if KEEP_DATA:
            logger.debug("Flag KEEP_DATA is set, uploading files.")
            try:
                # The KEEP_DATA setting is used only for debugging purposes so the
                # task should not be used to transfer very large files.
                # We wait for up to 10 minutes for it to complete.
                send_coroutine = self.upload_handler.send_file_descriptors(
                    [str(file) for file in Path("./").rglob("*") if file.is_file()]
                )
                await asyncio.wait_for(send_coroutine, timeout=600)
            except Exception:
                await log_exception(
                    "Unable to upload the data (KEEP_DATA flag is set).",
                    self.communicator,
                )

        # Send the finish command to the communication container.
        logger.debug("Send the finish command to the communication container.")
        await self.communicator.finish(self.return_code)

        # Stop the heartbeat task.
        logger.debug("Stopping the heartbeat task.")
        await stop_task(self.heartbeat_task)

        # Finally, stop the communicator task.
        with suppress(Exception):
            logger.debug("Stopping communication with the communication container.")
            self.protocol_handler.stop_communicate()
            await asyncio.wait_for(self.communicator_task, timeout=stop_timeout)
        logger.debug("All tasks stopped.")

    @property
    def return_code(self):
        """Get the return code.

        :return: return the script task return code. When the script task is
            still running return 1.
        """
        task = self.protocol_handler.script_task
        if task is None or task.result() is None:
            return 1
        return task.result()

    async def run(self) -> int:
        """Start the main method.

        :raises RuntimeError: in case of failure.
        """
        try:
            await asyncio.wait(
                [self.protocol_handler.script_started.wait(), self._terminating.wait()],
                timeout=SCRIPT_START_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._terminating.is_set():
                logger.debug("Terminating message received.")
            elif not self.protocol_handler.script_started.is_set():
                await log_error(
                    f"The script failed to start in {SCRIPT_START_TIMEOUT} seconds.",
                    self.communicator,
                )
            else:
                monitored_tasks = (
                    self.script_server.wait_closed(),
                    self.upload_handler.upload_task,
                    self.communicator_task,
                    self._terminating.wait(),
                )
                # Stop processing when any of the following tasks complete.
                await asyncio.wait(monitored_tasks, return_when=asyncio.FIRST_COMPLETED)
        except Exception:
            await log_exception(
                "Unexpected exception in the processing container.", self.communicator
            )
        finally:
            # Send the return code to the communicate container.
            await self._stop_all_tasks()
            return self.return_code


class UploadHandler:
    """Handle upload of files via communication container."""

    def __init__(
        self, upload_socket: socket.socket, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Initialize local variables."""
        self._log_buffers = {"JSON": bytearray(), "STDOUT": bytearray()}
        self._log_files = {"JSON": JSON_LOG_PATH, "STDOUT": STDOUT_LOG_PATH}

        self.upload_socket = upload_socket
        self.exported_files_mapper: Dict[str, str] = dict()
        self.upload_task: Optional[asyncio.Task] = None
        lock_arguments: Dict[str, asyncio.AbstractEventLoop] = dict()
        if sys.version_info < (3, 7):
            lock_arguments = {"loop": loop}
        self._uploading_log_files_lock = asyncio.Lock(**lock_arguments)
        self._send_file_descriptors_lock = asyncio.Lock(**lock_arguments)
        self.loop = loop

    async def export_files(self, file_names: Sequence[str]):
        """Handle export files."""
        unique_names = []
        for file_name in file_names:
            unique_name = "export_" + uuid.uuid4().hex
            shutil.move(file_name, unique_name)
            unique_names.append(unique_name)
        presigned_urls = await self.send_file_descriptors(
            unique_names, need_presigned_urls=True, storage_name="upload"
        )
        for file_name, presigned_url in zip(file_names, presigned_urls):
            self.exported_files_mapper[file_name] = presigned_url

    def update_stdout(self, data: bytes):
        """Write data bytes to stdout."""
        self._log_buffers["STDOUT"] += data

    def update_jsonout(self, data: bytes):
        """Write data bytes to jsonout."""
        self._log_buffers["JSON"] += data

    def start_log_upload(self) -> asyncio.Task:
        """Start the timer that periodically uploads the log files.

        On multiple calls already created task is returned.
        """
        if self.upload_task is None or self.upload_task.done():
            self.upload_task = asyncio.ensure_future(self._update_logs_timer())
        return self.upload_task

    async def stop_log_upload(self):
        """Stop the timer that periodically uploads the log files."""
        if self.upload_task is not None:
            # Only cancel the task when uploading is not in progress.
            async with self._uploading_log_files_lock:
                await stop_task(self.upload_task)

    async def _update_logs_timer(self, stop_after: int = 3):
        """Update log files every UPDATE_LOG_FILES_TIMEOUT secods.

        :attr stop_after: the integer indication how many exceptions must occur
            during log upload to terminate the timer task.

        :raises Exception: when stop_after exceptions occur during transfer the
            last exception is re-raised.
        """
        exception_count = 0
        while True:
            try:
                await asyncio.sleep(UPDATE_LOG_FILES_TIMEOUT)
                logger.debug("Uploading log timer triggered.")
                await self.upload_log_files()
            except asyncio.CancelledError:
                logger.debug("Update log files timer canceled.")
                break
            except Exception:
                logger.exception("Error uploading log files.")
                exception_count += 1
                if exception_count >= stop_after:
                    raise

    async def upload_log_files(self):
        """Upload log files if needed.

        :raises BrokenPipeError: when upload socket is not available.
        """
        to_upload = []
        for log_type in ["JSON", "STDOUT"]:
            if self._log_buffers[log_type]:
                to_upload.append(
                    self._log_files[log_type].relative_to(WORKING_DIRECTORY)
                )
                with self._log_files[log_type].open("ba") as log_file:
                    log_file.write(self._log_buffers[log_type])
                    self._log_buffers[log_type].clear()

        if to_upload:
            logger.debug("Uploading log files: %s.", to_upload)
            async with self._uploading_log_files_lock:
                await self.send_file_descriptors(to_upload)

    async def send_file_descriptors(
        self,
        filenames: Sequence[str],
        need_presigned_urls: bool = False,
        storage_name: str = "data",
    ):
        """Send file descriptors over UNIX sockets.

        Code is based on the sample in manual
        https://docs.python.org/3/library/socket.html#socket.socket.sendmsg .

        :returns: the list of presigned urls for filenames (in the same order)
            if need_presigned_urls is set to True. Otherwise empty list is
            returned.

        :raises RuntimeError: on failure.
        """

        def receive(length: int) -> bytes:
            """Receive the length bytes from the upload socket."""
            received = 0
            message = b""
            while received != length:
                select.select([self.upload_socket], [], [])
                payload = self.upload_socket.recv(length - received)
                if payload == b"":
                    raise RuntimeError("Error reading data from the upload socket.")
                message += payload
                received = len(message)
            return message

        def send(payload: bytes):
            """Send the payload over the upload socket."""
            bytes_sent = 0
            to_send = len(payload)
            while bytes_sent < to_send:
                select.select([], [self.upload_socket], [])
                chunk_size = self.upload_socket.send(payload)
                bytes_sent += chunk_size
                payload = payload[chunk_size:]
                if chunk_size == 0:
                    raise RuntimeError("Error sending data to the upload socket.")

        def send_chunk(processing_filenames: Sequence[str]) -> List[str]:
            """Send chunk of filenames to uploader.

            :raises RuntimeError: on failure.
            :returns: the list of presigned urls (if requested) or empty list.
            """
            logger.debug("Sending file descriptors for files: %s", processing_filenames)
            message = [storage_name, processing_filenames, need_presigned_urls]
            payload = json.dumps(message).encode()

            # File handlers must be created before file descriptors otherwise
            # garbage collector will kick in and close the handlers. Handlers
            # will be closed when function completes.
            file_handlers = [open(filename, "rb") for filename in processing_filenames]
            file_descriptors = [file_handler.fileno() for file_handler in file_handlers]
            send(len(payload).to_bytes(8, byteorder="big"))
            select.select([], [self.upload_socket], [])
            self.upload_socket.sendmsg(
                [payload],
                [
                    (
                        socket.SOL_SOCKET,
                        socket.SCM_RIGHTS,
                        array.array("i", file_descriptors),
                    )
                ],
            )
            response_length = int.from_bytes(receive(8), byteorder="big")
            response = json.loads(receive(response_length).decode())
            if not response["success"]:
                raise RuntimeError(
                    "Communication container response indicates error sending "
                    "files {}.".format(processing_filenames)
                )
            return response["presigned_urls"]

        result = []
        self.upload_socket.setblocking(False)
        async with self._send_file_descriptors_lock:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                for i in range(0, len(filenames), DESCRIPTOR_CHUNK_SIZE):
                    processing_filenames = [
                        str(file_) for file_ in filenames[i : i + DESCRIPTOR_CHUNK_SIZE]
                    ]
                    response = await self.loop.run_in_executor(
                        pool, send_chunk, processing_filenames
                    )
                    if need_presigned_urls:
                        result += response
        return result

    def close(self):
        """Close the upload socket."""
        self.upload_socket.close()


class ScriptProtocol(BaseProtocol):
    """Process the processing script messages."""

    def __init__(
        self,
        communication_communicator: SocketCommunicator,
        script_communicator: SocketCommunicator,
        upload_handler: UploadHandler,
    ):
        """Initialize."""
        super().__init__(script_communicator, logger)
        self.communication_communicator = communication_communicator
        self.upload_handler = upload_handler

    async def process_command(
        self, peer_identity: PeerIdentity, received_message: Message
    ):
        """Log the received message and call the parents handler."""
        self.upload_handler.update_jsonout(
            json.dumps(received_message.to_dict()).encode()
        )
        return await super().process_command(peer_identity, received_message)

    async def handle_export_files(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Export files by sending them to the communication container."""
        await self.upload_handler.export_files(message.message_data)
        return message.respond_ok("")

    async def handle_upload_files(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Upload files by sending them to the communication container."""
        filenames = message.message_data
        await self.upload_handler.send_file_descriptors(filenames)
        return message.respond_ok("")

    async def handle_run(
        self, message: Message, identity: PeerIdentity
    ) -> Response[str]:
        """Handle the run command."""
        command_data = {
            "data": message.message_data,
            "export_files_mapper": self.upload_handler.exported_files_mapper,
        }
        command = Message.command(message.command_name, command_data)
        return await self.communication_communicator.send_command(command)

    async def default_command_handler(
        self, message: Message, identity: PeerIdentity
    ) -> Response:
        """By default proxy all commands to the communication container."""
        return await self.communication_communicator.send_command(message)


def sig_term_handler(processing_manager: ProcessingManager):
    """Gracefully terminate the running process."""
    logger.debug("SIG_INT received, shutting down.")
    processing_manager.terminate()


async def init(loop: asyncio.AbstractEventLoop) -> ProcessingManager:
    """Create necessary objects."""
    # Create temporary directory inside working directory.
    temporary_directory = WORKING_DIRECTORY / TMP_DIR
    temporary_directory.mkdir(exist_ok=True)

    # Create (empty) log files.
    STDOUT_LOG_PATH.touch()
    JSON_LOG_PATH.touch()

    # Create the processing manager.
    processing_manager = await initialize_connections(loop)

    # Addd signal handler for SIGINT and SIGTERM.
    loop.add_signal_handler(
        signal.SIGINT, partial(sig_term_handler, processing_manager)
    )
    loop.add_signal_handler(
        signal.SIGTERM, partial(sig_term_handler, processing_manager)
    )

    return processing_manager


if __name__ == "__main__":
    # Replace the code bellow with asyncio.run when we no longer have to
    # support python 3.6.
    with closing(asyncio.get_event_loop()) as loop:
        processing_manager = loop.run_until_complete(init(loop))
        return_code = loop.run_until_complete(processing_manager.run())
    sys.exit(return_code)
