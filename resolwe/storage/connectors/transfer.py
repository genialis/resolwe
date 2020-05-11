"""Data transfer between connectors."""
import concurrent.futures
import logging
from contextlib import suppress
from functools import partial
from typing import List

import wrapt

from .baseconnector import BaseStorageConnector
from .circular_buffer import CircularBuffer
from .exceptions import DataTransferError
from .hasher import StreamHasher

try:
    from google.api_core.exceptions import ServiceUnavailable
    from requests.exceptions import ConnectionError, ReadTimeout

    gcs_exceptions = [ConnectionError, ReadTimeout, ServiceUnavailable]
except ModuleNotFoundError:
    gcs_exceptions = []


logger = logging.getLogger(__name__)
ERROR_MAX_RETRIES = 3
transfer_exceptions = tuple(gcs_exceptions + [DataTransferError])


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
        self, from_connector: BaseStorageConnector, to_connector: BaseStorageConnector,
    ):
        """Initialize transfer object."""
        self.from_connector = from_connector
        self.to_connector = to_connector

    def transfer_rec(self, url: str, objects: List[str] = None):
        """Transfer all objects under the given URL.

        Objects are read from to_connector and copied to from_connector. This
        could cause significant number of operations to a storage provider
        since it could lists all the objects in the url.

        :param url: the given URL to transfer from/to.
        :type url: str

        :param objects: the list of objects to transfer. Their paths are
            relative with respect to url. When the argument is not given a
            list of objects is obtained from the connector.
        :type objects: List[str]
        """
        if objects is None:
            objects = self.from_connector.get_object_list(url)
        for entry in objects:
            # Do not transfer directories.
            if entry[-1] != "/":
                self.transfer(entry, entry)

    @retry_on_transfer_error
    def transfer(self, from_url: str, to_url: str):
        """Transfer single object between two storage connectors."""

        def future_done(stream_to_close, future):
            stream_to_close.close()
            if future.exception() is not None:
                executor.shutdown(wait=False)

        hash_stream = CircularBuffer()
        data_stream = CircularBuffer()
        hasher_chunk_size = 8 * 1024 * 1024

        hasher = StreamHasher(hasher_chunk_size)
        download_hash_type = self.from_connector.supported_download_hash[0]
        upload_hash_type = self.to_connector.supported_upload_hash[0]

        # Hack for S3/local connector to use same chunk size for transfer and
        # hash calculation (affects etag calculation).
        if hasattr(self.to_connector, "multipart_chunksize"):
            hasher_chunk_size = self.to_connector.multipart_chunksize

        # Check if file already exist and has the right hash.
        to_hashes = self.to_connector.supported_download_hash
        from_hashes = self.from_connector.supported_download_hash
        common_hash = [e for e in to_hashes if e in from_hashes]
        if common_hash:
            hash_type = common_hash[0]
            from_hash = self.from_connector.get_hash(from_url, hash_type)
            to_hash = self.to_connector.get_hash(to_url, hash_type)
            if from_hash == to_hash and from_hash is not None:
                # Object exists and has the right hash.
                logger.debug(
                    "From: {}:{}".format(self.from_connector.name, from_url)
                    + " to: {}:{}".format(self.to_connector.name, to_url)
                    + " object exists with right hash, skipping."
                )
                return

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            download_task = executor.submit(
                self.from_connector.get, from_url, hash_stream
            )
            hash_task = executor.submit(hasher.compute, hash_stream, data_stream)
            upload_task = executor.submit(self.to_connector.push, data_stream, to_url)
            download_task.add_done_callback(partial(future_done, hash_stream))
            hash_task.add_done_callback(partial(future_done, data_stream))
            futures = (download_task, hash_task, upload_task)

        if any(f.exception() is not None for f in futures):
            with suppress(Exception):
                self.to_connector.delete(to_url)
            ex = [f.exception() for f in futures if f.exception() is not None]
            messages = [str(e) for e in ex]
            raise DataTransferError("\n\n".join(messages))

        from_hash = self.from_connector.get_hash(from_url, download_hash_type)
        to_hash = self.to_connector.get_hash(to_url, upload_hash_type)

        hasher_from_hash = hasher.hexdigest(download_hash_type)
        hasher_to_hash = hasher.hexdigest(upload_hash_type)

        if (from_hash, to_hash) != (hasher_from_hash, hasher_to_hash):
            with suppress(Exception):
                self.to_connector.delete(to_url)
            raise DataTransferError()

        # Store computed hashes as metadata for later use.
        hashes = {
            hash_type: hasher.hexdigest(hash_type)
            for hash_type in StreamHasher.KNOWN_HASH_TYPES
        }
        # This is strictly speaking not a hash but is set to know the value
        # of upload_chunk_size for awss3etag computation.
        hashes["_upload_chunk_size"] = str(hasher.chunk_size)
        self.to_connector.set_hashes(to_url, hashes)
