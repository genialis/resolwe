"""Transfer missing files to executor."""
import logging
import os
import sys
import time

from .global_settings import DATA_META
from .manager_commands import send_manager_command
from .protocol import ExecutorProtocol

logger = logging.getLogger(__name__)

# Make sure sphinx can import this module.
try:
    from .connectors import Transfer, connectors
    from .connectors.exceptions import DataTransferError
except ImportError:
    logger.exception("Unable to import 'connectors' module")


DOWNLOAD_WAITING_TIMEOUT = 60  # in seconds
RETRIES = 5


async def transfer_data():
    """Transfer missing data, terminate script on failure."""
    if not await _transfer_data():
        await send_manager_command(
            ExecutorProtocol.UPDATE,
            extra_fields={
                ExecutorProtocol.UPDATE_CHANGESET: {
                    "process_error": ["Failed to transfer data."],
                    "status": DATA_META["STATUS_ERROR"],
                }
            },
        )
        await send_manager_command(ExecutorProtocol.ABORT, expect_reply=False)
        sys.exit(1)


async def _transfer_data():
    """Fetch missing data.

    Get a list of missing storage locations from the manager fetch
    data using appropriate storage connectors.
    """
    result = await send_manager_command(ExecutorProtocol.MISSING_DATA_LOCATIONS)
    data_to_transfer = result[ExecutorProtocol.STORAGE_DATA_LOCATIONS]

    to_connector = connectors["local"]
    base_path = to_connector.base_path
    data_downloading = []

    # Notify manager to change status of the data object.
    if data_to_transfer:
        await send_manager_command(
            ExecutorProtocol.UPDATE,
            extra_fields={
                ExecutorProtocol.UPDATE_CHANGESET: {
                    "status": DATA_META["STATUS_PREPARING"]
                }
            },
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
            reply = await send_manager_command(
                ExecutorProtocol.DOWNLOAD_STARTED,
                extra_fields={
                    ExecutorProtocol.STORAGE_LOCATION_ID: storage_location_id,
                    ExecutorProtocol.DOWNLOAD_STARTED_LOCK: True,
                },
            )
            result = reply[ExecutorProtocol.DOWNLOAD_RESULT]
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
                if not await download_data(missing_data):
                    logger.error(
                        "Download for data with id {} aborted".format(
                            missing_data["data_id"]
                        )
                    )
                    return False
            elif result == ExecutorProtocol.DOWNLOAD_IN_PROGRESS:
                # Somebody else is downloading the data, wait for it
                logger.debug(
                    "Download for storage location with id {} already in progress".format(
                        storage_location_id
                    )
                )
                data_downloading.append(missing_data)

        elif data_downloading:
            logger.debug("Waiting for downloads {} to finish".format(data_downloading))
            time.sleep(DOWNLOAD_WAITING_TIMEOUT)
            data_downloading_new = []
            for download in data_downloading:
                storage_location_id = download["to_storage_location_id"]
                # Query for the status of the download
                reply = await send_manager_command(
                    ExecutorProtocol.DOWNLOAD_STARTED,
                    extra_fields={
                        ExecutorProtocol.STORAGE_LOCATION_ID: storage_location_id,
                        ExecutorProtocol.DOWNLOAD_STARTED_LOCK: False,
                    },
                )
                result = reply[ExecutorProtocol.DOWNLOAD_RESULT]
                # We have 3 options:
                # - download was aborted, we can restart it
                # - download is still in progress
                # - download finished
                if result == ExecutorProtocol.DOWNLOAD_STARTED:
                    # We can start the download
                    data_to_transfer.append(download)
                    logger.debug("Remote download {} was aborted".format(download))
                elif result == ExecutorProtocol.DOWNLOAD_IN_PROGRESS:
                    # Download is still in progress, wait
                    data_downloading_new.append(download)
                    logger.debug("Download {} is still in progress".format(download))
            data_downloading = data_downloading_new
    return True


async def download_data(missing_data: dict) -> bool:
    """Download missing data for one data object.

    :returns: status of the transfer: True for success, False for failure.
    :rtype: bool
    """
    data_id = missing_data["data_id"]
    logger.debug(
        "Starting data transfer for data_id {}".format(missing_data["data_id"])
    )
    objects = None
    for retry in range(1, RETRIES + 1):
        try:
            access_log_id = None
            to_connector = connectors["local"]
            from_connector = connectors[missing_data["connector_name"]]
            from_storage_location_id = missing_data["from_storage_location_id"]
            to_storage_location_id = missing_data["to_storage_location_id"]
            logger.debug(
                "Locking storage location with id {}".format(from_storage_location_id)
            )
            response = await send_manager_command(
                ExecutorProtocol.STORAGE_LOCATION_LOCK,
                extra_fields={
                    ExecutorProtocol.STORAGE_LOCATION_ID: from_storage_location_id,
                    ExecutorProtocol.STORAGE_LOCATION_LOCK_REASON: "Executor data transfer",
                },
            )
            access_log_id = response[ExecutorProtocol.STORAGE_ACCESS_LOG_ID]

            if objects is None:
                response = await send_manager_command(
                    ExecutorProtocol.GET_FILES_TO_DOWNLOAD,
                    extra_fields={
                        ExecutorProtocol.STORAGE_LOCATION_ID: from_storage_location_id,
                    },
                )
                objects = response[ExecutorProtocol.REFERENCED_FILES]

            t = Transfer(from_connector, to_connector)
            t.transfer_objects(missing_data["url"], objects)
            await send_manager_command(
                ExecutorProtocol.DOWNLOAD_FINISHED,
                extra_fields={
                    ExecutorProtocol.STORAGE_LOCATION_ID: to_storage_location_id
                },
            )
            return True
        except DataTransferError:
            logger.exception(
                "Data transfer error downloading data with id {}, retry {} out of".format(
                    data_id, retry, RETRIES
                )
            )
        except Exception:
            logger.exception(
                "Unknown error downloading data with id {}, retry {} out of".format(
                    data_id, retry, RETRIES
                )
            )
        finally:
            if access_log_id is not None:
                await send_manager_command(
                    ExecutorProtocol.STORAGE_LOCATION_UNLOCK,
                    expect_reply=False,
                    extra_fields={
                        ExecutorProtocol.STORAGE_ACCESS_LOG_ID: access_log_id
                    },
                )
    # None od the retries has been successfull, abort the download.
    await send_manager_command(
        ExecutorProtocol.DOWNLOAD_ABORTED,
        expect_reply=False,
        extra_fields={ExecutorProtocol.STORAGE_LOCATION_ID: to_storage_location_id},
    )
    return False
