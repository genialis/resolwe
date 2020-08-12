"""Processing container startup script."""
import asyncio
import functools
import json
import logging
import os
import shlex
import shutil
import signal
import sys
import uuid
from contextlib import suppress
from pathlib import Path

from socket_utils import (
    BaseProtocol,
    Message,
    PeerIdentity,
    Response,
    SocketCommunicator,
    async_receive_data,
    async_retry,
    async_send_data,
)

# The directory where local processing is done.
DATA_LOCAL_VOLUME = Path(os.environ.get("DATA_LOCAL_VOLUME", "/data_local"))
DATA_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data"))
DATA_ALL_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data_all"))
TMP_DIR = Path(os.environ.get("TMP_DIR", ".tmp"))

# Basic constants.
STDOUT_LOG_PATH = DATA_VOLUME / "stdout.txt"
JSON_LOG_PATH = DATA_VOLUME / "jsonout.txt"


# Read configuration from environmental variables.
COMMUNICATION_CONTAINER_TIMEOUT = int(os.environ.get("CONTAINER_TIMEOUT", 300))
SOCKETS_PATH = Path(os.environ.get("SOCKETS_VOLUME", "/sockets"))
COMMUNICATION_SOCKET = SOCKETS_PATH / os.environ.get(
    "COMMUNICATION_PROCESSING_SOCKET", "_socket1.s"
)
SCRIPT_SOCKET = SOCKETS_PATH / os.environ.get("SCRIPT_SOCKET", "_socket2.s")

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ProtocolHandler(BaseProtocol):
    """Handles communication with the communication container."""

    def __init__(self, communicator):
        # type: (SocketCommunicator) -> None
        """Initialization."""
        super().__init__(communicator, logger)
        self._script_future = None  # type: Optional[asyncio.Future] # noqa: F821
        self.return_code = 0

    async def handle_process_script(self, message, identity):
        # type: (Message, PeerIdentity) -> Response[int]
        """Process the given script."""
        self._script_future = asyncio.ensure_future(
            self._execute_script(message.message_data)
        )
        await self._script_future
        # The next line might raise exception. It will be handled by the
        # communicator and error response will be sent back together with the
        # error details.
        self.return_code = self._script_future.result()
        return message.respond_ok(self.return_code)

    async def handle_terminate(self, message, identity):
        # type: (Message, PeerIdentity) -> Response[str]
        """Stop script processing and communication."""
        if self._script_future is not None:
            self._script_future.cancel()
            with suppress(asyncio.CancelledError):
                await self._script_future
        self.stop_communicate()
        return await super().handle_terminate(message, identity)

    async def _execute_script(self, script):
        # type: (str) -> int
        """Execute given bash script in another process.

        :returns: the return code of the script.
        """
        # Start the bash subprocess and write script to its standard input.
        logger.debug(f"Executing script: {script}")
        bash_command = "/bin/bash --login" + os.linesep
        proc = await asyncio.subprocess.create_subprocess_exec(
            *shlex.split(bash_command),
            limit=4 * (2 ** 20),  # 4MB buffer size for line buffering
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdin is not None
        assert proc.stdout is not None
        proc.stdin.write(script.encode("utf-8"))
        proc.stdin.close()
        logger.debug(f"Script sent to stdin.")

        # Read and log the standard output. If exception is thrown it will
        # be handled in the SocketCommunicator.
        with STDOUT_LOG_PATH.open("bw") as stdout_file:
            received = await proc.stdout.readline()
            while received:
                stdout_file.write(received)
                received = await proc.stdout.readline()
                stdout_file.flush()

        await proc.wait()
        assert proc.returncode is not None
        logger.debug(f"Script finished with rc {proc.returncode}")
        return proc.returncode


class ProcessingManager:
    """Manager class.

    It starts network servers and executes the received script.
    """

    def __init__(self):
        """Initialization."""
        self.worker_reader = None  # type: Optional[asyncio.StreamReader] # noqa: F821
        self.worker_writer = None  # type: Optional[asyncio.StreamWriter] # noqa: F821
        self.worker_connected = asyncio.Event()
        self.exported_files_mapper = dict()  # type: Dict[str, str] # noqa: F821

    @async_retry(
        min_sleep=1,
        max_sleep=1,
        max_retries=COMMUNICATION_CONTAINER_TIMEOUT,
        retry_exceptions=(Exception,),
    )
    async def _wait_for_container(self, path):
        # type: (str) -> SocketCommunicator
        r"""Wait for container to start.

        Assumption is that container has server running on the given port and
        will send a single message b"PING\n" when connected.

        :raises: when unable to connect for COMMUNICATOR_WAIT_TIMEOUT seconds.
        """
        reader, writer = await asyncio.open_unix_connection(path)
        try:
            line = (await reader.readline()).decode("utf-8")
            assert line.strip() == "PING"
            return SocketCommunicator(reader, writer, "communication_container", logger)
        except:
            with suppress(Exception):
                writer.close()
                await writer.wait_closed()
            raise

    async def run(self):
        # type: () -> int
        """Start the main method."""
        try:
            logger.debug("Waiting for the processing container to connect.")
            try:
                self.communicator = await self._wait_for_container(
                    os.fspath(COMMUNICATION_SOCKET)
                )
                logger.info("Connected to the processing container.")
            except:
                logger.exception(
                    "Can not connect to the communication container for "
                    f"{COMMUNICATION_CONTAINER_TIMEOUT} seconds."
                )
                return 1

            protocol_handler = ProtocolHandler(self.communicator)

            # Accept network connections from the processing script.
            await asyncio.start_unix_server(
                self._handle_processing_script_connection,
                os.fspath(SCRIPT_SOCKET),
            )

            # Start the protocol.
            await protocol_handler.communicate()
        except:
            logger.exception("Exception while running startup script")
        finally:
            return protocol_handler.return_code

    async def _handle_export_files(self, data):
        # type: (dict) -> None
        """Handle export files."""
        for file_name in data:
            # TODO: get settings in here!!!!
            export_folder = Path("/upload")  # SETTINGS["FLOW_EXECUTOR"]["UPLOAD_DIR"]
            unique_name = "export_{}".format(uuid.uuid4().hex)
            export_path = export_folder / unique_name
            self.exported_files_mapper[file_name] = unique_name
            shutil.move(file_name, export_path)

    async def _handle_processing_script_connection(self, reader, writer):
        # type: (asyncio.StreamReader, asyncio.StreamWriter) -> None
        """Handle incoming connection from the processing script.

        The requests are just proxied to the communication container with
        the exception of the .

        Python process starts a single connection while Resolwe runtime utils
        starts a new connection for every request.
        """
        try:
            logger.debug("Processing script connected to socket.")
            with JSON_LOG_PATH.open("at") as json_file:
                logger.debug("Receiving data from the processing script")
                received = await async_receive_data(reader)
                while received:
                    _, message_dict = received
                    json_file.write(json.dumps(message_dict) + os.linesep)
                    message = Message.from_dict(message_dict)
                    logger.debug(f"Processing script sent message: {message}")
                    if message.command_name == "export_files":
                        await self._handle_export_files(message.message_data)
                        response = message.respond_ok("OK")
                    elif message.command_name == "run":
                        message.message_data = {
                            "data": message.message_data,
                            "export_files_mapper": self.exported_files_mapper,
                        }
                        response = await self.communicator.send_command(message)
                    else:
                        response = await self.communicator.send_command(message)
                    await async_send_data(writer, response.to_dict())
                    logger.debug("Receiving data from the processing script")
                    received = await async_receive_data(reader)
        except asyncio.IncompleteReadError:
            logger.info(
                "Incomplete read error while receiving data from processing script."
            )
        except:
            logger.exception("Exception while handling processing script message")
        finally:
            writer.close()


def sig_term_handler(processing_task):
    # type: (asyncio.Future) -> None
    """Gracefully terminate the running process."""
    logger.debug("SIG_INT received, shutting down.")
    processing_task.cancel()


async def start_processing_container():
    # type: () -> None
    """Start the processing manager and set signal handlers."""
    processing_manager = ProcessingManager()
    manager_future = asyncio.ensure_future(processing_manager.run())

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGINT, functools.partial(sig_term_handler, manager_future)
    )

    # Wait for the manager task to finish.
    return await manager_future


if __name__ == "__main__":
    # Change working directory into /data_local and create .tmp directory
    # inside it.
    (DATA_LOCAL_VOLUME / TMP_DIR).mkdir(exist_ok=True)
    os.chdir(DATA_LOCAL_VOLUME)

    # Consider replacing tho code bellew with asyncio.run when we no longer
    # have to support python 3.6.
    loop = asyncio.get_event_loop()
    sys.exit(loop.run_until_complete(start_processing_container()))
    loop.close()
