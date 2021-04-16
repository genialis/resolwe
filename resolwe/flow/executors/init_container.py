"""Kubernetes init container initialization script.

1. Fix permission (make genialis user owner) on subdirectories on the EBS
   volume used for local processing.
2. Transfer missing data to the input EBS volume.
"""

import asyncio
import logging
import os
import shutil
import sys
from collections import defaultdict
from contextlib import suppress
from typing import Any

import zmq
import zmq.asyncio
from executors import constants, global_settings
from executors.connectors import Transfer, connectors
from executors.connectors.baseconnector import BaseStorageConnector
from executors.connectors.exceptions import DataTransferError
from executors.connectors.utils import paralelize
from executors.protocol import ExecutorFiles
from executors.socket_utils import BaseCommunicator, BaseProtocol, Message, PeerIdentity
from executors.zeromq_utils import ZMQCommunicator

from .transfer import transfer_data

logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


LISTENER_IP = os.getenv("LISTENER_SERVICE_HOST", "127.0.0.1")
LISTENER_PORT = os.getenv("LISTENER_SERVICE_PORT", "53893")
LISTENER_PROTOCOL = os.getenv("LISTENER_PROTOCOL", "tcp")

DATA_ID = int(os.getenv("DATA_ID", "-1"))


GENIALIS_UID = int(os.environ.get("GENIALIS_UID", 0))
GENIALIS_GID = int(os.environ.get("GENIALIS_GID", 0))
MOUNTED_CONNECTORS = [
    name for name in os.environ["MOUNTED_CONNECTORS"].split(",") if name
]

DOWNLOAD_WAITING_TIMEOUT = 60  # in seconds
RETRIES = 5


class PreviousDataExistsError(Exception):
    """Raised if data from previous run exists."""


async def transfer_inputs(communicator: BaseCommunicator, missing_data: dict):
    """Transfer missing input data.

    :raises DataTransferError: on failure.
    """
    inputs_connector = connectors["_input"]
    # Group files by connectors. So we can transfer files from single connector
    # in parallel. We could also transfer files belonging to different
    # connectors in parallel but this could produce huge number of threads,
    # since S3 uses multiple threads to transfer single file.
    objects_to_transfer: dict[str, list[dict[str, str]]] = defaultdict(list)
    for base_url, missing_item in missing_data.items():
        for file in missing_item["files"]:
            file.update({"from_base_url": base_url, "to_base_url": base_url})
        objects_to_transfer[missing_item["from_connector"]] += missing_item["files"]

    try:
        for connector_name in objects_to_transfer:
            await download_to_location(
                objects_to_transfer[connector_name],
                connectors[connector_name],
                inputs_connector,
            )
    except:
        error_message = (
            f"Preparing inputs {objects_to_transfer[connector_name]} from "
            f"connector {connector_name} failed."
        )
        await error(error_message, communicator)
        raise DataTransferError(error_message)


async def download_to_location(
    files: list[dict[str, str]],
    from_connector: BaseStorageConnector,
    to_connector: BaseStorageConnector,
    max_threads: int = 5,
):
    """Download missing paths.

    :raises DataTransferError: on failure.
    """
    logger.info(f"Transfering data {from_connector} --> {to_connector}.")
    transfer = Transfer(from_connector, to_connector)
    # Start futures and evaluate their results. If exception occured it will
    # be re-raised.
    for future in paralelize(
        objects=files,
        worker=lambda objects: transfer.transfer_chunk(None, objects),
        max_threads=max_threads,
    ):
        future.result()


def check_for_previous_data():
    """Check if previous data exists.

    This method must check processing volume and all mounted shared volumes
    for stdout file.

    :raises PreviousDataExistsError: when data from previous run exists.
    """
    paths_to_check = [
        constants.PROCESSING_VOLUME / global_settings.LOCATION_SUBPATH / "stdout.txt"
    ]
    data_storage = global_settings.SETTINGS["FLOW_STORAGE"]["data"]
    for connector_name in data_storage["connectors"]:
        connector = connectors[connector_name]
        if connector.mountable:
            paths_to_check.append(
                connector.path / global_settings.LOCATION_SUBPATH / "stdout.txt"
            )
    if any(path.is_file() for path in paths_to_check):
        raise PreviousDataExistsError(
            "File 'stdout.txt' exists from previous run, aborting processing."
        )


def prepare_volumes():
    """Create necessary folders and set permissions.

    Prepare a folder LOCATION_SUBPATH inside processing volume and set the
    applicable owner and permissions.
    """
    check_for_previous_data()
    for volume in [constants.PROCESSING_VOLUME, constants.INPUTS_VOLUME]:
        if volume.is_dir():
            logger.debug("Preparing %s.", volume)
            directory = volume / global_settings.LOCATION_SUBPATH
            directory_mode = global_settings.SETTINGS.get("FLOW_EXECUTOR", {}).get(
                "DATA_DIR_MODE", 0o755
            )
            directory.mkdir(mode=directory_mode, exist_ok=True)
            with suppress(PermissionError):
                shutil.chown(directory, GENIALIS_UID, GENIALIS_GID)


def _get_communicator() -> ZMQCommunicator:
    """Connect to the listener."""
    zmq_context = zmq.asyncio.Context.instance()
    zmq_socket = zmq_context.socket(zmq.DEALER)
    zmq_socket.setsockopt(zmq.IDENTITY, str(DATA_ID).encode())
    connect_string = f"{LISTENER_PROTOCOL}://{LISTENER_IP}:{LISTENER_PORT}"
    logger.debug("Opening connection to %s", connect_string)
    zmq_socket.connect(connect_string)
    return ZMQCommunicator(zmq_socket, "init_container <-> listener", logger)


def initialize_secrets(secrets: dict[str, Any]):
    """Initialize secrets."""
    for file_name, content in secrets.items():
        with (constants.SECRETS_VOLUME / file_name).open("wt") as stream:
            stream.write(content)


class InitProtocol(BaseProtocol):
    """Protocol class."""

    async def post_terminate(self, message: Message, identity: PeerIdentity):
        """Handle post-terminate command."""
        logger.debug("Init container received terminate request, terminating.")
        await error("Init container received terminating request.", self.communicator)
        for task in asyncio.all_tasks():
            task.cancel()

    async def transfer_missing_data(self):
        """Transfer missing data.

        :raises DataTransferError: when data transfer error occurs.
        """
        await self.communicator.update_status("PP")
        missing_data = (await self.communicator.missing_data_locations("")).message_data

        await self.communicator.init_suspend_heartbeat("")

        to_filesystem = {
            url: entry | {"url": url}
            for url, entry in missing_data.items()
            if "to_connector" in entry
        }
        to_inputs = {
            url: entry
            for url, entry in missing_data.items()
            if "to_connector" not in entry
        }

        if to_inputs and "_input" not in connectors:
            raise RuntimeError("No inputs volume is defined.")
        if to_filesystem and to_inputs:
            raise RuntimeError(
                "Can only transfer data to input volume or shared storage, not both."
            )

        if to_inputs:
            transfering_coroutine = transfer_inputs(self.communicator, to_inputs)
        else:
            transfering_coroutine = transfer_data(
                self.communicator, list(to_filesystem.values())
            )
        await transfering_coroutine


async def error(error_message: str, communicator: BaseCommunicator):
    """Error occured inside container.

    Send the error and terminate the process.
    """
    with suppress(Exception):
        await communicator.process_log({"error": error_message})


def modify_connector_settings():
    """Modify mountpoints and add processing and input connectors.

    The value of the key 'path' in config dictionary points to the path on the
    worker node. It has to be remaped to a path inside container.
    Also add processing and input connector settings.
    """
    connector_settings = global_settings.SETTINGS["STORAGE_CONNECTORS"]
    storage_settings = global_settings.SETTINGS["FLOW_STORAGE"]
    connector_storage = {
        connector_name: storage_name
        for storage_name in storage_settings
        for connector_name in storage_settings[storage_name]["connectors"]
    }

    # Point connector path to the correct mountpoint.
    for connector_name in MOUNTED_CONNECTORS:
        storage_name = connector_storage[connector_name]
        connector_settings[connector_name]["config"][
            "path"
        ] = f"/{storage_name}_{connector_name}"

    connector_settings["_processing"] = {
        "connector": "executors.connectors.localconnector.LocalFilesystemConnector",
        "config": {"path": constants.PROCESSING_VOLUME},
    }
    if constants.INPUTS_VOLUME_NAME in global_settings.SETTINGS["FLOW_VOLUMES"]:
        connector_settings["_input"] = {
            "connector": "executors.connectors.localconnector.LocalFilesystemConnector",
            "config": {"path": constants.INPUTS_VOLUME},
        }


async def main():
    """Start the main program.

    :raises RuntimeError: when runtime error occurs.
    :raises asyncio.exceptions.CancelledError: when task is terminated.
    """
    communicator = _get_communicator()
    protocol = InitProtocol(communicator, logger)
    communicate_task = asyncio.ensure_future(protocol.communicate())

    # Initialize settings constants by bootstraping.
    response = await communicator.send_command(
        Message.command("bootstrap", (DATA_ID, "init"))
    )
    global_settings.initialize_constants(DATA_ID, response.message_data)
    initialize_secrets(response.message_data[ExecutorFiles.SECRETS_DIR])
    modify_connector_settings()
    connectors.recreate_connectors()

    try:
        prepare_volumes()
        await protocol.transfer_missing_data()
    except PreviousDataExistsError:
        logger.warning("Previous run data exists, aborting processing.")
        raise
    except Exception as exception:
        message = f"Unexpected exception in init container: {exception}."
        logger.exception(message)
        await error(message, communicator)
        raise
    finally:
        protocol.stop_communicate()
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(communicate_task, timeout=10)


if __name__ == "__main__":
    logger.debug("Starting the main program.")
    try:
        asyncio.run(main())
    except PreviousDataExistsError:
        sys.exit(2)
    except:
        sys.exit(1)
