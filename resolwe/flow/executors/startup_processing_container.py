"""Processing container startup script."""
import array
import asyncio
import concurrent
import functools
import json
import logging
import os
import shlex
import shutil
import signal
import socket
import sys
import time
import uuid
from contextlib import suppress
from distutils.util import strtobool
from pathlib import Path

import constants

# The directory where local processing is done.
TMP_DIR = Path(os.environ.get("TMP_DIR", ".tmp"))
DATA_ID = int(os.getenv("DATA_ID", "-1"))

# Basic constants.
STDOUT_LOG_PATH = constants.PROCESSING_VOLUME / "stdout.txt"
JSON_LOG_PATH = constants.PROCESSING_VOLUME / "jsonout.txt"

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


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


assert sys.version_info[0] == 3, "Only Python3 is supported"
assert sys.version_info[1] >= 4, "Only Python >= 3.4 is supported"

if sys.version_info[1] == 4:
    # This is done to awoid syntax error in Python >= 3.7
    create_task = getattr(asyncio, "async")
else:
    create_task = asyncio.ensure_future


def respond(message, status, response_data):
    """Return a response to the given message."""
    if message["type"] in ["COMMAND", "EQR"]:
        response_type = "RESPONSE"
    else:
        response_type = "HBTR"
    response = {"type": response_type}
    response["type_data"] = status
    response["data"] = response_data
    response["uuid"] = message.get("uuid", uuid.uuid4().hex)
    if "sequence_number" in message:
        response["sequence_number"] = message["sequence_number"]
    response["timestamp"] = time.time()
    return response


class ProtocolHandler:
    """Handles communication with the communication container."""

    def __init__(self, manager):
        # type: (ProcessingManager) -> None
        """Initialize."""
        self._script_future = None  # type: Optional[asyncio.Future] # noqa: F821
        self.return_code = 0
        self.manager = manager
        self.communicator = manager.communicator
        self._script_finishing = None
        self._response_queue = list()
        self._command_lock = asyncio.Lock()
        self._message_in_queue = asyncio.Event()
        self._command_counter = 0

    @asyncio.coroutine
    def communicate(self):
        """Listen for messages from communication container."""
        while True:
            message = yield from self.communicator.receive_data()
            if message is None:
                break
            if message["type"] == "HBT":
                logger.debug("Responding to HBT.")
                yield from self.communicator.send_data(respond(message, "OK", ""))
            elif message["type"] == "EQR":
                logger.debug("Responding to EQR.")
                yield from self.communicator.send_data(
                    respond(message, "OK", "received")
                )
            elif message["type"] == "COMMAND":
                command_name = message["type_data"]
                if command_name == "process_script":
                    if self._script_future is not None:
                        response = respond(message, "ERR", "Already processing")
                        yield from self.communicator.send_data(response)
                    else:
                        create_task(self.process_script(message))
                elif command_name == "terminate":
                    logger.debug("Got terminate command, shutting down.")
                    self._command_counter += 1
                    yield from self.terminate_script(
                        {"info": "Processing was cancelled."}
                    )
                    response = respond(message, "OK", "")
                    yield from self.communicator.send_data(response)
                    # Wait for script to do the clean-up.
                    yield from self._script_finishing.wait()
                    break
                else:
                    logger.error("Unknown command: %s.", command_name)
                    response = respond(message, "ERR", "Unknown command")
                    yield from self.communicator.send_data(response)
                    break
            elif message["type"] == "RESPONSE":
                self._response_queue.append(message)
                self._message_in_queue.set()

        logger.debug("Communication with communication container stopped.")

    @asyncio.coroutine
    def send_command(self, command):
        """Send a command and return a response."""
        logger.debug("Sending command %s.", command)
        with (yield from self._command_lock):
            self._command_counter += 1
            command["uuid"] = uuid.uuid4().hex
            command["sequence_number"] = self._command_counter
            command["timestamp"] = time.time()
            try:
                assert len(self._response_queue) == 0
                self._message_in_queue.clear()
                yield from self.communicator.send_data(command)
                yield from self._message_in_queue.wait()
                return self._response_queue.pop()
            except:
                logger.exception("Unknown exception in send_command.")
                raise

    @asyncio.coroutine
    def _send_log(self, message):
        """Send log message."""
        log_command = {
            "type": "COMMAND",
            "type_data": "process_log",
            "data": message,
        }
        yield from self.send_command(log_command)

    @asyncio.coroutine
    def process_script(self, message):
        # type: (dict) -> ()
        """Process the given script."""
        self.return_code = 1
        self._script_future = create_task(self._execute_script(message["data"]))
        try:
            yield from self._script_future
            self.return_code = self._script_future.result()
        finally:
            self._script_future = None
            try:
                logger.debug("Uploading latest log files.")
                try:
                    yield from self.manager.upload_log_files()
                except:
                    yield from self._send_log(
                        {"error": "Error uploading final log files."}
                    )
                    logger.exception("Error uploading final log files.")
                    if self.return_code == 0:
                        self.return_code = -1

                if KEEP_DATA:
                    try:
                        yield from self.manager.send_file_descriptors(
                            [
                                str(file)
                                for file in Path("./").rglob("*")
                                if file.is_file()
                            ]
                        )
                    except:
                        logger.exception("Error sending file descriptors (keep_data).")
                        yield from self._send_log(
                            {
                                "error": "Error uploading final file descriptors (keep_data)."
                            }
                        )
                        if self.return_code == 0:
                            self.return_code = -1

                response = respond(message, "OK", self.return_code)
                yield from self.communicator.send_data(response)

            except:
                logger.exception(
                    "Unexpected exception in process_script finally clause."
                )
            finally:
                # Close the cliet socket to notify communicator that processing is
                # finished.
                logger.debug("Closing upload socket.")
                self.manager.upload_socket.close()
                logger.debug("Upload socket closed.")
                if self._script_finishing:
                    logger.debug("Setting script finishing.")
                    self._script_finishing.set()

    @asyncio.coroutine
    def terminate_script(self, message=None):
        """Terminate the running script and optionally send log message."""
        try:
            if message is not None:
                yield from self._send_log(message)
        except:
            logger.exception("Error sending terminating message to listener.")
        finally:
            if self._script_future is not None:
                logger.debug("Terminating script")
                self._script_finishing = asyncio.Event()
                self._script_future.cancel()

    @asyncio.coroutine
    def _execute_script(self, script):
        # type: (str) -> int
        """Execute given bash script.

        :returns: the return code of the script.
        """
        # Start the bash subprocess and write script to its standard input.
        logger.debug("Executing script: %s.", script)
        bash_command = "/bin/bash --login" + os.linesep

        # The following comment commands blact to not reformat the code
        # block. It must not have trailing comma due compatibility with
        # Python 3.4.

        # fmt: off
        proc = yield from asyncio.subprocess.create_subprocess_exec(
            *shlex.split(bash_command),
            limit=4 * (2 ** 20),  # 4MB buffer size for line buffering
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        # fmt: on
        assert proc.stdin is not None
        assert proc.stdout is not None
        proc.stdin.write(script.encode("utf-8"))
        proc.stdin.close()
        logger.debug("Script sent to stdin.")

        try:
            received = yield from proc.stdout.readline()
            while received:
                with (yield from self.manager._uploading_log_files_lock):
                    with STDOUT_LOG_PATH.open("ab") as stdout_file:
                        stdout_file.write(received)
                        self.manager.log_files_need_upload = True
                received = yield from proc.stdout.readline()
        except asyncio.CancelledError:
            # Task was cancelled: terminate the running subprocess immediately.
            proc.kill()
        finally:
            yield from proc.wait()
            assert proc.returncode is not None
            logger.debug("Script finished with rc %d.", proc.returncode)
            return proc.returncode


class Communicator:
    """Simple communication class."""

    def __init__(self, reader, writer):
        """Initialize."""
        self.reader = reader
        self.writer = writer

    @asyncio.coroutine
    def send_data(self, data, size_bytes=8):
        # type: (dict, int) -> None
        """Send data over socket.

        :param s: socket to send over.
        :param data: dict that must be serialzable to JSON.
        :param size_bytes: how first many bytes in message are dedicated to the
            message size (send as binary number).
        :raises: exception on failure.
        """
        payload = json.dumps(data).encode("utf-8")
        size = len(payload).to_bytes(size_bytes, byteorder="big")
        self.writer.write(size)
        self.writer.write(payload)
        yield from self.writer.drain()

    @asyncio.coroutine
    def receive_data(self, size_bytes=8):
        # type: (socket.SocketType, int) -> Optional[Dict] # noqa: F821
        """Recieve data over the given socket.

        :param size_bytes: how first many bytes in message are dedicated to the
            message size (binary number).
        """
        with suppress(asyncio.IncompleteReadError):
            received = yield from self.reader.readexactly(size_bytes)
            message_size = int.from_bytes(received, byteorder="big")
            received = yield from self.reader.readexactly(message_size)
            assert len(received) == message_size
            return json.loads(received.decode("utf-8"))


class ProcessingManager:
    """Manager class.

    It starts network servers and executes the received script.
    """

    def __init__(self, loop):
        """Initialize."""
        self.worker_reader = None  # type: Optional[asyncio.StreamReader] # noqa: F821
        self.worker_writer = None  # type: Optional[asyncio.StreamWriter] # noqa: F821
        self.protocol_handler = None  # type: Optional[BaseProtocol] # noqa: F821
        self.worker_connected = asyncio.Event()
        self.exported_files_mapper = dict()  # type: Dict[str, str] # noqa: F821
        self.upload_socket = None
        self._uploading_log_files_lock = asyncio.Lock(loop=loop)
        self._send_file_descriptors_lock = asyncio.Lock(loop=loop)
        self.log_files_need_upload = False
        self.loop = loop

    @asyncio.coroutine
    def _wait_for_communication_container(self, path):
        # type: (str) -> Communicator
        r"""Wait for communication container to start.

        Assumption is that the communication container is accepting connections
        and will send a single b"PING\n" when connected.

        :raises: if unable to connect for COMMUNICATOR_WAIT_TIMEOUT seconds.
        """
        for _ in range(constants.CONTAINER_TIMEOUT):
            try:
                reader, writer = yield from asyncio.open_unix_connection(path)
                line = (yield from reader.readline()).decode("utf-8")
                assert line.strip() == "PING"
                return Communicator(reader, writer)
            except:
                with suppress(Exception):
                    writer.close()
                    yield from writer.wait_closed()
                time.sleep(1)
        raise RuntimeError("Communication container is unreacheable.")

    @asyncio.coroutine
    def run(self):
        # type: () -> int
        """Start the main method."""
        try:
            logger.debug("Connecting to the communication container.")
            try:
                self.communicator = yield from self._wait_for_communication_container(
                    str(COMMUNICATION_SOCKET)
                )
                logger.info("Connected to the communication container.")
            except:
                logger.exception(
                    "Could not connect to the communication container for "
                    "%d seconds.",
                    constants.CONTAINER_TIMEOUT,
                )
                return 1

            self.protocol_handler = ProtocolHandler(self)

            # Connect to the upload socket.
            self.upload_socket = socket.socket(family=socket.AF_UNIX)
            logger.debug("Connecting upload socket to: %s.", UPLOAD_FILE_SOCKET)
            self.upload_socket.connect(str(UPLOAD_FILE_SOCKET))
            logger.debug("Upload socket connected %s.", UPLOAD_FILE_SOCKET)

            # Accept network connections from the processing script.
            yield from asyncio.start_unix_server(
                self._handle_processing_script_connection,
                str(SCRIPT_SOCKET),
            )

            # Update log files.
            update_logs_future = create_task(self.update_log_files_timer())

            # Await messages from the communication controler. The command
            # stops when socket is closed or communication is stopped.
            yield from self.protocol_handler.communicate()

        except:
            logger.exception("Exception while running startup script.")
        finally:
            update_logs_future.cancel()
            # Just in case connection was closed before script was finished.
            yield from self.protocol_handler.terminate_script()
            logger.debug("Stopping processing container.")
            return self.protocol_handler.return_code

    @asyncio.coroutine
    def _handle_export_files(self, file_names):
        # type: (dict) -> None
        """Handle export files."""
        unique_names = []
        for file_name in file_names:
            unique_name = "export_" + uuid.uuid4().hex
            shutil.move(file_name, unique_name)
            unique_names.append(unique_name)
        presigned_urls = yield from self.send_file_descriptors(
            unique_names, need_presigned_urls=True, storage_name="upload"
        )
        for file_name, presigned_url in zip(file_names, presigned_urls):
            self.exported_files_mapper[file_name] = presigned_url

    @asyncio.coroutine
    def update_log_files_timer(self):
        """Update log files every UPDATE_LOG_FILES_TIMEOUT secods."""
        while True:
            try:
                yield from self.upload_log_files()
            except:
                logger.exception("Error uploading log files.")
                yield from self.protocol_handler.terminate_script(
                    {"error": "Error uploading log files."}
                )
            yield from asyncio.sleep(UPDATE_LOG_FILES_TIMEOUT)

    @asyncio.coroutine
    def upload_log_files(self):
        """Upload log files if needed.

        :raises BrokenPipeError: when upload socket is not available.
        """
        if self.log_files_need_upload:
            log_paths = [
                log_file.relative_to(constants.PROCESSING_VOLUME)
                for log_file in [STDOUT_LOG_PATH, JSON_LOG_PATH]
            ]
            logger.debug("Timer uploading log files %s.", log_paths)
            with (yield from self._uploading_log_files_lock):
                yield from self.send_file_descriptors(log_paths)
            self.log_files_need_upload = False

    @asyncio.coroutine
    def send_file_descriptors(
        self, filenames, need_presigned_urls=False, storage_name="data"
    ):
        # type: (Tuple(str, Union[List[str], PurePath], bool)) -> List  # noqa: F821
        """Send file descriptors over UNIX sockets.

        Code is based on the sample in manual
        https://docs.python.org/3/library/socket.html#socket.socket.sendmsg .

        :returns: the list of presigned urls for filenames (in the same order)
            if need_presigned_urls is set to True. Otherwise empty list is
            returned.

        :raises RuntimeError: on failure.
        """

        def send_chunk(processing_filenames):
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
            self.upload_socket.sendall(len(payload).to_bytes(8, byteorder="big"))
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
            response_length = int.from_bytes(
                self.upload_socket.recv(8), byteorder="big"
            )
            response = json.loads(self.upload_socket.recv(response_length).decode())
            if not response["success"]:
                raise RuntimeError(
                    "Communication container response indicates error sending "
                    "files {}.".format(processing_filenames)
                )
            return response["presigned_urls"]

        result = []
        with (yield from self._send_file_descriptors_lock):
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                for i in range(0, len(filenames), DESCRIPTOR_CHUNK_SIZE):
                    processing_filenames = [
                        str(file_) for file_ in filenames[i : i + DESCRIPTOR_CHUNK_SIZE]
                    ]
                    response = yield from self.loop.run_in_executor(
                        pool, send_chunk, processing_filenames
                    )
                    if need_presigned_urls:
                        result += response
        return result

    @asyncio.coroutine
    def _handle_processing_script_connection(self, reader, writer):
        # type: (asyncio.StreamReader, asyncio.StreamWriter) -> None
        """Handle incoming connection from the processing script.

        The requests are just proxied to the communication container with
        the exception of the .

        Python process starts a single connection while Resolwe runtime utils
        starts a new connection for every request.
        """
        try:
            communicator = Communicator(reader, writer)
            logger.debug("Processing script connected to socket.")

            message = yield from communicator.receive_data()
            while message:
                with (yield from self._uploading_log_files_lock):
                    with JSON_LOG_PATH.open("at") as json_file:
                        json_file.write(json.dumps(message) + os.linesep)
                        self.log_files_need_upload = True
                logger.debug("Processing script sent message: %s.", message)
                command = message["type_data"]
                if command == "export_files":
                    yield from self._handle_export_files(message["data"])
                    response = respond(message, "OK", "")
                elif command == "upload_files":
                    # Connect to the upload socket.
                    filenames = message["data"]
                    logger.debug("Sending filedescriptors.")
                    # This can block, run in a thread maybe?
                    logger.debug("Names: %s", filenames)
                    yield from self.send_file_descriptors(filenames)
                    logger.debug("Print descriptors sent.")
                    response = respond(message, "OK", "")
                elif command == "run":
                    message["data"] = {
                        "data": message["data"],
                        "export_files_mapper": self.exported_files_mapper,
                    }
                    response = yield from self.protocol_handler.send_command(message)
                else:
                    response = yield from self.protocol_handler.send_command(message)

                # Terminate the script on ERROR response.
                if response["type_data"] == "ERR":
                    logger.debug("Response with error status received, terminating")
                    # Terminate the script and wait for termination.
                    # Otherwise script may continue to run and produce
                    # hard to debug error messages.
                    yield from self.protocol_handler.terminate_script(
                        {"info": "Response with status 'ERROR' received from listener."}
                    )
                    if self.protocol_handler._script_finishing is not None:
                        yield from self.protocol_handler._script_finishing.wait()
                        break
                else:
                    yield from communicator.send_data(response)
                message = yield from communicator.receive_data()

        except (asyncio.IncompleteReadError, GeneratorExit):
            logger.info("Processing script closed connection.")
        except:
            logger.exception(
                "Unknown exception while handling processing script message."
            )
            yield from self.protocol_handler.terminate_script(
                {
                    "error": "Unexpected exception while handling processing script message."
                }
            )
            yield from self.protocol_handler._script_finishing.wait()
        finally:
            writer.close()


@asyncio.coroutine
def sig_term_handler(processing_manager):
    # type: (asyncio.Future) -> None
    """Gracefully terminate the running process."""
    logger.debug("SIG_INT received, shutting down.")
    yield from processing_manager.protocol_handler.terminate_script(
        {"error": "SIG_INT received, terminating."}
    )


@asyncio.coroutine
def start_processing_container(loop):
    # type: () -> None
    """Start the processing manager and set SIGINT handler."""
    processing_manager = ProcessingManager(loop)
    manager_future = create_task(processing_manager.run())

    asyncio.get_event_loop().add_signal_handler(
        signal.SIGINT,
        functools.partial(create_task, sig_term_handler(processing_manager)),
    )

    # Wait for the manager task to finish.
    return (yield from manager_future)


if __name__ == "__main__":
    # Change working directory into /data_local and create .tmp directory
    # inside it.
    tmp_path = constants.PROCESSING_VOLUME / TMP_DIR
    if not tmp_path.is_dir():
        (constants.PROCESSING_VOLUME / TMP_DIR).mkdir()
    os.chdir(str(constants.PROCESSING_VOLUME))

    # Create log files.
    STDOUT_LOG_PATH.touch()
    JSON_LOG_PATH.touch()

    # Replace the code bellow with asyncio.run when we no longer have to
    # support python 3.6.
    loop = asyncio.get_event_loop()
    return_code = loop.run_until_complete(start_processing_container(loop))
    loop.close()
    sys.exit(return_code)
