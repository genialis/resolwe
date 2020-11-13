"""Communication container startup script."""

import asyncio
import functools
import logging
import os
import shutil
import signal
import sys
from contextlib import suppress
from distutils.util import strtobool
from pathlib import Path
from typing import Optional

import zmq
import zmq.asyncio
from executors.collect import collect_files
from executors.socket_utils import (
    BaseCommunicator,
    BaseProtocol,
    Message,
    PeerIdentity,
    Response,
    SocketCommunicator,
)
from executors.transfer import transfer_data
from executors.zeromq_utils import ZMQCommunicator

# Socket used to connect with the processing container.
SOCKETS_PATH = Path(os.getenv("SOCKETS_VOLUME", "/sockets"))
PROCESSING_SOCKET = SOCKETS_PATH / os.getenv(
    "COMMUNICATION_PROCESSING_SOCKET", "_socket1.s"
)
PROCESSING_CONTAINER_TIMEOUT = int(os.getenv("CONTAINER_TIMEOUT", 300))

# Listener IP and port are read from environment.
LISTENER_IP = os.getenv("LISTENER_IP", "127.0.0.1")
LISTENER_PORT = os.getenv("LISTENER_PORT", "53893")
LISTENER_PROTOCOL = os.getenv("LISTENER_PROTOCOL", "tcp")
DATA_ID = int(os.getenv("DATA_ID", "-1"))
KEEP_DATA = bool(strtobool(os.environ.get("FLOW_MANAGER_KEEP_DATA", "False")))


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
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
        for root, dirs, files in os.walk(os.environ.get("SECRETS_DIR", "/secrets")):
            for f in files:
                os.chmod(os.path.join(root, f), 0o700)
                os.unlink(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d), onerror=handle_error)

    except OSError:
        logger.exception("Manager exception while removing data runtime directory.")


class ListenerProtocol(BaseProtocol):
    """Listener protocol."""

    def __init__(
        self, communicator: BaseCommunicator, processing_communicator: BaseCommunicator
    ):
        """Initialization."""
        super().__init__(communicator, logger)
        self.processing_communicator = processing_communicator

    async def get_script(self) -> str:
        """Get the script from the listener."""
        response = await self.communicator.send_command(
            Message.command("get_script", "")
        )
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
        """Initialization."""
        super().__init__(communicator, logger)
        self.listener_communicator = listener_communicator

    async def default_command_handler(
        self, message: Message, identity: PeerIdentity
    ) -> Response:
        """Proxy command to the listener."""
        return await self.listener_communicator.send_command(message, identity)

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
        """Initialization."""
        self.processing_communicator: Optional[BaseCommunicator] = None
        self.listener_communicator: Optional[BaseCommunicator] = None
        self.processing_container_connected = asyncio.Event()
        self._process_script_task: Optional[asyncio.Task] = None

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

    async def collect_produced_files(self):
        """Collect files produced by the worker.

        Keep only files that are referenced in the data model.
        Log error re-raise exception on failure.

        :raises: RuntimeError on failure.
        """
        try:
            logger.debug("Collecting files")
            await collect_files(self.listener_communicator)
            logger.debug("Collected files")
            return True
        except RuntimeError:
            with suppress(Exception):
                await self.listener_communicator.send_command(
                    Message.command(
                        "process_log",
                        {"error": ["Error collecting produced files."]},
                    )
                )
        return False

    def _communicator_stopped(self, future: asyncio.Future):
        """Stop processing if necessary."""
        if self._process_script_task:
            logger.debug("Communicator closed, cancelling script processing.")
            self._process_script_task.cancel()

    async def start(self) -> int:
        """Start the main program."""
        try:
            return_code = 1
            await self.start_processing_socket()
            self.listener_communicator = await self.open_listener_connection()
            try:
                logger.debug("Waiting for the processing container to connect")
                await asyncio.wait_for(
                    self.processing_container_connected.wait(),
                    PROCESSING_CONTAINER_TIMEOUT,
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
                processing_task = asyncio.ensure_future(processing.communicate())
                listener_task.add_done_callback(self._communicator_stopped)
                processing_task.add_done_callback(self._communicator_stopped)

                await self.listener_communicator.send_command(
                    Message.command("update_status", "PR")
                )
                await self.transfer_missing_data()

                script = await listener.get_script()
                self._process_script_task = asyncio.create_task(
                    processing.process_script(script)
                )
                return_code = await self._process_script_task
                self._process_script_task = None

            except RuntimeError:
                logger.exception("Error processing script.")
                with suppress(Exception):
                    await self.listener_communicator.send_command(
                        Message.command(
                            "process_log",
                            {"error": ["Runtime error in communication container."]},
                        )
                    )

        except Exception:
            logger.exception("While running communication container")

        finally:
            if not KEEP_DATA:
                purge_secrets()

            if not await self.collect_produced_files():
                if return_code == 0:
                    return_code = 1

            # Notify listener that the processing is finished.
            with suppress(Exception):
                await listener.finish(return_code)

            listener.stop_communicate()
            processing.stop_communicate()

            # Wait for up to 10 seconds to close the tasks.
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(listener_task, processing_task), timeout=10
                )
            return return_code


def sig_term_handler(manager_task: asyncio.Task):
    # type: (asyncio.Future) -> None
    """Gracefully terminate the running process."""
    logger.debug("SIG_INT received, shutting down.")
    manager_task.cancel()


async def start_communication_container():
    # type: () -> None
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
