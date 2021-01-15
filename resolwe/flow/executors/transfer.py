"""Transfer missing files to executor."""
import asyncio
import concurrent
import logging
import os

from .protocol import ExecutorProtocol
from .socket_utils import BaseCommunicator, Message

logger = logging.getLogger(__name__)

# Make sure sphinx can import this module.
try:
    from .connectors import Transfer, connectors
    from .connectors.exceptions import DataTransferError
except ImportError:
    from resolwe.storage.connectors import Transfer, connectors
    from resolwe.storage.connectors.exceptions import DataTransferError


DOWNLOAD_WAITING_TIMEOUT = 60  # in seconds
RETRIES = 5


async def transfer_data(communicator: BaseCommunicator):
    """Transfer missing data.

    :raises RuntimeError: on failure.
    """
    if not await _transfer_data(communicator):
        raise RuntimeError("Failed to transfer data")


async def _transfer_data(communicator: BaseCommunicator):
    """Fetch missing data.

    Get a list of missing storage locations from the manager fetch
    data using appropriate storage connectors.
    """
    response = await communicator.send_command(
        Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")
    )

    data_to_transfer = response.message_data
    to_connector = connectors["local"]
    base_path = to_connector.base_path

    data_downloading: list = []

    # Notify manager to change status of the data object.
    if data_to_transfer:
        await communicator.send_command(
            Message.command("update_status", "PP")  # TODO: DATA_META before
        )
    # Start downloading data.
    while data_to_transfer or data_downloading:
        if data_to_transfer:
            missing_data = data_to_transfer.pop()
            storage_location_id = missing_data["to_storage_location_id"]
            logger.debug(
                "Requesting download lock for storage location with id {}".format(
                    storage_location_id
                )
            )
            response = await communicator.send_command(
                Message.command(
                    ExecutorProtocol.DOWNLOAD_STARTED,
                    {
                        ExecutorProtocol.STORAGE_LOCATION_ID: storage_location_id,
                        ExecutorProtocol.DOWNLOAD_STARTED_LOCK: True,
                    },
                )
            )
            result = response.message_data
            # There are three options:
            # - download is already finished
            # - we can start downloading
            # - download is already in progress
            # The first one needs no special processing.
            if result == ExecutorProtocol.DOWNLOAD_STARTED:
                # We are good to go for download
                logger.debug(
                    "Download lock for storage location with id {} obtained".format(
                        storage_location_id
                    )
                )
                dest_dir = os.path.join(base_path, missing_data["url"])
                os.makedirs(dest_dir, exist_ok=True)
                # Download data will abort or finish the download
                if not await download_data(missing_data, communicator):
                    logger.error(
                        "Download for data with id {} aborted".format(
                            missing_data["data_id"]
                        )
                    )
                    return False
            elif result == ExecutorProtocol.DOWNLOAD_IN_PROGRESS:
                # Somebody else is downloading the data, wait for it
                logger.debug(
                    "Download for storage location with id %d already in progress",
                    storage_location_id,
                )
                data_downloading.append(missing_data)

        elif data_downloading:
            logger.debug("Waiting for downloads %s to finish", data_downloading)
            await asyncio.sleep(DOWNLOAD_WAITING_TIMEOUT)
            data_downloading_new = []
            for download in data_downloading:
                storage_location_id = download["to_storage_location_id"]
                # Query for the status of the download
                response = await communicator.send_command(
                    Message.command(
                        ExecutorProtocol.DOWNLOAD_STARTED,
                        {
                            ExecutorProtocol.STORAGE_LOCATION_ID: storage_location_id,
                            ExecutorProtocol.DOWNLOAD_STARTED_LOCK: False,
                        },
                    )
                )
                # We have 3 options:
                # - download was aborted, we can restart it
                # - download is still in progress
                # - download finished
                if response.message_data == ExecutorProtocol.DOWNLOAD_STARTED:
                    # We can start the download
                    data_to_transfer.append(download)
                    logger.debug("Remote download %s was aborted", download)
                elif response.message_data == ExecutorProtocol.DOWNLOAD_IN_PROGRESS:
                    # Download is still in progress, wait
                    data_downloading_new.append(download)
                    logger.debug("Download %s is still in progress", download)
            data_downloading = data_downloading_new
    return True


async def download_data(missing_data: dict, communicator: BaseCommunicator) -> bool:
    """Download missing data for one data object.

    :returns: status of the transfer: True for success, False for failure.
    :rtype: bool
    """
    data_id = missing_data["data_id"]
    logger.debug("Starting data transfer for data_id %d", missing_data["data_id"])
    objects = None
    for retry in range(1, RETRIES + 1):
        try:
            to_connector = connectors["local"]
            from_connector = connectors[missing_data["connector_name"]]
            from_storage_location_id = missing_data["from_storage_location_id"]
            to_storage_location_id = missing_data["to_storage_location_id"]
            logger.debug(
                "Locking storage location with id %d", from_storage_location_id
            )

            if objects is None:
                response = await communicator.send_command(
                    Message.command(
                        ExecutorProtocol.GET_FILES_TO_DOWNLOAD, from_storage_location_id
                    )
                )
                objects = response.message_data

            # Execute long running task in a threadpool.
            loop = asyncio.get_event_loop()
            with concurrent.futures.ThreadPoolExecutor() as pool:
                t = Transfer(from_connector, to_connector)
                await loop.run_in_executor(
                    pool, t.transfer_objects, missing_data["url"], objects
                )

            await communicator.send_command(
                Message.command(
                    ExecutorProtocol.DOWNLOAD_FINISHED,
                    to_storage_location_id,
                )
            )
            return True
        except DataTransferError:
            logger.exception(
                "Data transfer error downloading data with id {}, retry {}/{}".format(
                    data_id, retry, RETRIES
                )
            )
        except Exception:
            logger.exception(
                "Unknown error downloading data with id {}, retry {}/{}".format(
                    data_id, retry, RETRIES
                )
            )
    # None od the retries has been successfull, abort the download.
    await communicator.send_command(
        Message.command(ExecutorProtocol.DOWNLOAD_ABORTED, to_storage_location_id)
    )
    return False
