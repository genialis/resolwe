"""Data transfer between connectors."""
import concurrent.futures
import logging
from contextlib import suppress
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, List, Optional, Union

import wrapt
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout

from .circular_buffer import CircularBuffer
from .exceptions import DataTransferError
from .utils import paralelize

if TYPE_CHECKING:
    from os import PathLike
    from .baseconnector import BaseStorageConnector

try:
    from google.api_core.exceptions import ServiceUnavailable

    gcs_exceptions = [ServiceUnavailable]
except ModuleNotFoundError:
    gcs_exceptions = []


logger = logging.getLogger(__name__)
ERROR_MAX_RETRIES = 3
transfer_exceptions = tuple(
    gcs_exceptions + [DataTransferError] + [RequestsConnectionError, ReadTimeout]
)


@wrapt.decorator
def retry_on_transfer_error(wrapped, instance, args, kwargs):
    """Retry on tranfser error."""
    for _ in range(ERROR_MAX_RETRIES):
        try:
            return wrapped(*args, **kwargs)

        except transfer_exceptions as err:
            connection_err = err

    raise connection_err


class Transfer:
    """Transfer data between two storage connectors using in-memory buffer."""

    def __init__(
        self,
        from_connector: "BaseStorageConnector",
        to_connector: "BaseStorageConnector",
    ):
        """Initialize transfer object."""
        self.from_connector = from_connector
        self.to_connector = to_connector
        # Always upload file larger that small_file (in bytes).
        self.small_file = 100_000

    def pre_processing(
        self, url: Union[str, Path], objects: Optional[List[dict]] = None
    ):
        """Notify connectors that transfer is about to start.

        The connector is allowed to change names of the objects that are to be
        transfered. This allows us to do some pre-processing, like zipping all
        files into one and transfering that one.

        :param url: base url for file transfer.

        :param objects: list of objects to be transfered, their paths are
            relative with respect to the url.
        """
        objects_to_transfer = self.from_connector.before_get(objects, url)
        self.to_connector.before_push(objects_to_transfer, url)
        return objects_to_transfer

    def post_processing(
        self, url: Union[str, Path], objects: Optional[List[dict]] = None
    ):
        """Notify connectors that transfer is complete.

        :param url: base url for file transfer.

        :param objects: the list ob objects that was actually transfered.The
            paths are relative with respect to the url.
        """
        self.from_connector.after_get(objects, url)
        objects_stored = self.to_connector.after_push(objects, url)
        return objects_stored

    def transfer_objects(
        self, url: Union[str, Path], objects: List[dict], max_threads: int = 10
    ) -> Optional[List[dict]]:
        """Transfer objects under the given URL.

        Objects are read from from_connector and copied to to_connector.

        :param url: the given URL to transfer from/to.

        :param objects: the list of objects to transfer. Each object is
            represented with the dictionary containing at least keys "path",
            "size", "md5", "crc32c", "awss3etag".
            All values for key "path" must be relative with respect to the
            argument url.

        :returns: the list of objects that were stored in the to_connector if
            it is different that argument objects or None.
        """
        # Pre-processing.
        try:
            objects_to_transfer = self.pre_processing(url, objects)
        except Exception:
            logger.exception(
                "Error in pre-processing while transfering data from url {}".format(url)
            )
            raise DataTransferError()

        url = Path(url)

        futures = paralelize(
            objects=objects_to_transfer,
            worker=partial(self._transfer_chunk, url),
            max_threads=max_threads,
        )

        # Check future results. This wil re-raise exception raised in
        # _transfer_chunk.
        for future in futures:
            future.result()

        # Post-processing.
        try:
            objects_stored = self.post_processing(url, objects_to_transfer)
        except Exception:
            logger.exception(
                "Error in post-processing while transfering data from url {}".format(
                    url
                )
            )
            raise DataTransferError()

        return None if objects_stored is objects else objects_stored

    def _transfer_chunk(self, url: Path, objects: Iterable[dict]):
        """Transfer a single chunk of objects."""
        to_connector = self.to_connector.duplicate()
        from_connector = self.from_connector.duplicate()
        for entry in objects:
            # Do not transfer directories.
            if not entry["path"].endswith("/"):
                self.transfer(
                    url, entry, url, Path(entry["path"]), from_connector, to_connector
                )

    def transfer(
        self,
        from_base_url: Union[str, Path],
        object_: dict,
        to_base_url: Union[str, Path],
        to_url: "PathLike[str]",
        from_connector: "BaseStorageConnector" = None,
        to_connector: "BaseStorageConnector" = None,
    ):
        """Transfer single object between two storage connectors.

        :param from_base_url: base url on from_connector.

        :param object_: object to transfer. It must be a dictionary containing
            at least keys "path", "md5", "crc32c", "size" and "awss3etag".

        :param to_base_url: base url on to_connector.

        :param to_url: where to copy object. It is relative with respect to the
            argument to_base_url.

        :param from_connector: from connector, defaults to None. If None
            duplicate of from_connector from the Transfer class instance is
            used.

        :param to_connector: to connector, defaults to None. If None
            duplicate of to_connector from the Transfer class instance is
            used.
        """
        to_base_url = Path(to_base_url)
        # Duplicate connectors for thread safety.
        to_connector = to_connector or self.to_connector.duplicate()
        from_connector = from_connector or self.from_connector.duplicate()

        from_url = Path(from_base_url) / object_["path"]
        hashes = {type_: object_[type_] for type_ in ["md5", "crc32c", "awss3etag"]}
        common_hash_type = next(
            e for e in to_connector.supported_hash if e in hashes.keys()
        )
        from_hash = hashes[common_hash_type]

        # Check if file already exist and has the right hash.
        # Skip check for small files as it is slower than upload.
        if object_["size"] > self.small_file:
            to_hash = to_connector.get_hash(to_base_url / to_url, common_hash_type)
            if from_hash == to_hash:
                # Object exists and has the right hash.
                logger.debug(
                    "From: {}:{}".format(from_connector.name, from_url)
                    + " to: {}:{}".format(to_connector.name, to_base_url / to_url)
                    + " object exists with right hash, skipping."
                )
                return

        # When object can be open directly as stream do it.
        if from_connector.can_open_stream:
            stream = from_connector.open_stream(from_url, "rb")
            to_connector.push(stream, to_base_url / to_url)
            stream.close()

        elif to_connector.can_open_stream:
            stream = to_connector.open_stream(to_base_url / to_url, "wb")
            from_connector.get(from_url, stream)
            stream.close()
        # Otherwise create out own stream and use threads to transfer data.
        else:

            def future_done(stream_to_close, future):
                stream_to_close.close()
                if future.exception() is not None:
                    executor.shutdown(wait=False)

            data_stream = CircularBuffer(
                buffer_size=min(200 * 1024 * 1024, object_["size"])
            )
            with concurrent.futures.ThreadPoolExecutor() as executor:
                download_task = executor.submit(
                    from_connector.get, from_url, data_stream
                )
                upload_task = executor.submit(
                    to_connector.push, data_stream, to_base_url / to_url
                )
                download_task.add_done_callback(partial(future_done, data_stream))
                futures = (download_task, upload_task)

            # Re-raise possible exception as DataTransferError.
            if any(f.exception() is not None for f in futures):
                # Log exceptions in threads to preserve original stack trace.
                for f in futures:
                    try:
                        f.result
                    except Exception:
                        logger.exception("Exception occured while transfering data")

                # Delete transfered data.
                with suppress(Exception):
                    to_connector.delete(to_base_url, [to_url])

                # Re-raise exception.
                ex = [f.exception() for f in futures if f.exception() is not None]
                messages = [str(e) for e in ex]
                raise DataTransferError("\n\n".join(messages))

        # Check hash of the uploaded object.
        if from_hash != to_connector.get_hash(to_base_url / to_url, common_hash_type):
            with suppress(Exception):
                to_connector.delete(to_base_url, [to_url])
            raise DataTransferError()

        # Store computed hashes as metadata for later use.
        # Value "_upload_chunk_size" not a hash but is set to know the value
        # of upload_chunk_size that was used for awss3etag computation.
        hashes["_upload_chunk_size"] = str(to_connector.CHUNK_SIZE)
        to_connector.set_hashes(to_base_url / to_url, hashes)
