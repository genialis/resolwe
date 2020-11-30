"""Communication container startup script."""

import asyncio
import logging
import os
import shutil
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

# Keep data settings.
KEEP_DATA = bool(strtobool(os.environ.get("FLOW_MANAGER_KEEP_DATA", "False")))

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

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

logger.info("Starting communication container for data with id %d.", DATA_ID)


def purge_secrets():
    """Purge the secrets directory."""

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

    def __init__(self, communicator: SocketCommunicator):
        """Initialization."""
        super().__init__(communicator, logger)

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
        logger.debug("Forwarding command to listener: %s, %s", message, identity)
        response = await self.listener_communicator.send_command(message, identity)
        logger.debug("Forwarding response: %s.", response)
        return response

    async def process_script(self, script: str) -> int:
        """Start processing the script in the processing container.

        Request the script from the listener and send it to the processing
        container for execution. Since the execution may take arbitrary long
        there is no timeout on waiting for the response.

        :returns: return code of the process running the script.
        """
        response = await self.communicator.send_command(
            Message.command("process_script", script), response_timeout=None
        )
        return response.message_data

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

    async def terminate(self, reason: str):
        """Terminate the process.

        If possible notify the processing container and the listener.
        """
        try:
            if self.listener_communicator is not None:
                await self.listener_communicator.send_command(
                    Message.command("terminating", reason)
                )
        except:
            logger.exception("Error sendind terminating command to the listener.")

        try:
            if self.processing_communicator is not None:
                await self.processing_communicator.send_command(
                    Message.command("terminate", reason)
                )
        except:
            logger.exception(
                "Error sendind terminating command to the processing container."
            )

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

    async def collect_produced_files(self) -> bool:
        """Collect files produced by the worker.

        When exception occurs it is caught, logged and sent to the listener.

        :returns: True on success, False on failure.
        """
        try:
            logger.debug("Collecting files")
            await collect_files(self.listener_communicator, KEEP_DATA)
            logger.debug("Collected files")
        except RuntimeError:
            logger.exception("Error collecting files")
            return False
        else:
            return True

    async def start(self) -> int:
        """Start the main program."""
        try:
            return_code = 1
            await self.start_processing_socket()
            logger.debug("Waiting for the processing container to connect")
            self.listener_communicator = await self.open_listener_connection()

            try:
                await asyncio.wait_for(
                    self.processing_container_connected.wait(),
                    PROCESSING_CONTAINER_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.exception(
                    "Connections to processing container can not be established. Terminating."
                )
                await self.terminate(
                    "Could not establish connection to the processing container."
                )
                sys.exit(1)

            logger.debug("Connected to the processing container.")

            listener = ListenerProtocol(self.listener_communicator)
            processing = ProcessingProtocol(
                self.processing_communicator, self.listener_communicator
            )
            listener_task = asyncio.ensure_future(listener.communicate())
            processing_task = asyncio.ensure_future(processing.communicate())

            try:
                await self.transfer_missing_data()

                await self.listener_communicator.send_command(
                    Message.command("update_status", "PR")
                )

                return_code = await processing.process_script(
                    await listener.get_script()
                )

            except RuntimeError:
                if return_code == 0:
                    return_code = 1
                logger.exception("Runtime error while running executor")
                with suppress(Exception):
                    await self.listener_communicator.send_command(
                        Message.command(
                            "process_log",
                            {
                                "error": [
                                    "Runtime error while running communication container"
                                ]
                            },
                        )
                    )
            finally:
                if not KEEP_DATA:
                    purge_secrets()

                if await self.collect_produced_files() is False:
                    if return_code == 0:
                        return_code = 1

                # Notify listener that the processing is finished.
                with suppress(Exception):
                    print("Waiting for listener to finish.")
                    await listener.finish(return_code)
                    print("Listener finished")

                # Send processing container the terminate command.
                with suppress(Exception):
                    print("Awaiting processing container to terminate.")
                    await processing.terminate()
                    print("Processing container terminated.")

                print("Stop communication.")
                listener.stop_communicate()
                processing.stop_communicate()
                print("Communication stopped.")

                print("Awaiting tasks to finish.")
                await listener_task
                print("Listener task finished.")
                await processing_task
                print("Processing task finished.")
        except Exception:
            logger.exception("While running communication container")
        finally:
            return return_code


if __name__ == "__main__":
    manager = Manager()
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(manager.start())
    logger.debug("Return code: %s", result)
    loop.close()
    logger.debug("Executor Loop closed")
    sys.exit(result)
