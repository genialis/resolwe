"""Data transfer between connectors."""
import concurrent.futures
import logging
from contextlib import suppress
from functools import partial
from threading import Condition
from typing import List, Optional

import wrapt

from .baseconnector import BaseStorageConnector
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


class CircularBuffer:
    """In-memory circular buffer."""

    DEFAULT_BUFFER_LENGTH = 1024 * 1024 * 200  # 200 MB
    DEFAULT_READ_WAIT_TIMEOUT = 10  # in seconds

    def __init__(
        self, buffer_size: int = DEFAULT_BUFFER_LENGTH, name: Optional[str] = None
    ):
        """Initialize circular buffer."""
        # + 1 since at least one element in the buffer must be empty
        self.__bs = buffer_size + 1
        # Construct a buffer
        self.__buffer = memoryview(bytearray(self.__bs))
        # Where to start reading from.
        self.__tail = 0
        # Where to start writing into.
        self.__head = 0
        self.__buffer_modify = Condition()
        # How many bytes user wants to read.
        self.__reading_bytes = 0
        # Is the stream closed.
        self.__closed = False
        self._bytes_read = 0
        self.name = name

    @property
    def closed(self) -> bool:
        """Is the stream closed.

        :returns: True if stream is closed, False otherwise.
        :rtype: bool
        """
        return self.__closed

    @property
    def buffer_size(self) -> int:
        """Get the size of the backing memory buffer.

        :return: number of bytes that can be stored in the memory buffer
            (in bytes).
        :rtype: int
        """
        return self.__bs - 1

    def __bytes_available(self) -> int:
        """Get the number of bytes availabe to consume."""
        free = self.__head - self.__tail
        return free if free >= 0 else free + self.__bs

    def __bytes_free(self) -> int:
        """Get the number of free bytes in the buffer."""
        return self.__bs - self.__bytes_available() - 1

    @property
    def __can_write(self) -> bool:
        """Is there any space available in the memory buffer."""
        return self.__bytes_free() > 0

    def read(self, size: int = None) -> bytes:
        """Read size bytes from the buffer.

        Blocks until there are not enought bytes available. If stream is
        closed then all available bytes are immediately returned.

        :param size: the number of bytes to read.
        :type size: int

        :return: data that was read from the stream.
        :rtype: bytes
        """
        if size is None:
            # Amazon reads without size parameter if it determines that the
            # stream is "small".
            # Just make it our entire buffer size, it should be closed anyway.
            size = CircularBuffer.DEFAULT_BUFFER_LENGTH
        ret = memoryview(bytearray(size))
        ret_pos = 0

        with self.__buffer_modify:
            while ret_pos < size:
                self.__reading_bytes = min(size, self.buffer_size)
                while (
                    self.__bytes_available() < self.__reading_bytes and not self.closed
                ):
                    self.__buffer_modify.wait(CircularBuffer.DEFAULT_READ_WAIT_TIMEOUT)
                bytes_to_read = min(self.__bytes_available(), size - ret_pos)
                slice_start = self.__tail
                slice_end = slice_start + bytes_to_read
                if slice_end <= self.__bs:
                    ret[ret_pos : ret_pos + bytes_to_read] = self.__buffer[
                        slice_start:slice_end
                    ]
                else:  # rotation
                    slice_end -= self.__bs
                    sep = self.__bs - slice_start
                    ret[ret_pos : ret_pos + sep] = self.__buffer[
                        slice_start : self.__bs
                    ]
                    ret[ret_pos + sep : ret_pos + sep + slice_end] = self.__buffer[
                        0:slice_end
                    ]
                self.__tail = slice_end
                ret_pos += bytes_to_read
                self._bytes_read += bytes_to_read
                # Notify the writing thread.
                self.__buffer_modify.notify()
                if self.closed:
                    break

            self.__reading_bytes = 0
            # Notify the writing thread.
            self.__buffer_modify.notify()
            return ret[0:ret_pos].tobytes()

    def write(self, data: bytes) -> int:
        """Write data into the stream.

        :param data: data to be writen to the stream.
        :type data: bytes

        :return: the number of bytes actually writen.
        :rtype: int
        """
        datamv = memoryview(data)
        data_pos = 0
        with self.__buffer_modify:
            # Wait for enough bytes to become available.
            # Abort if stream was closed.
            while data_pos < len(datamv) and not self.closed:
                while not self.__can_write and not self.closed:
                    self.__buffer_modify.wait()
                bytes_to_write = min(self.__bytes_free(), len(datamv) - data_pos)
                slice_start = self.__head
                slice_end = slice_start + bytes_to_write
                if slice_end <= self.__bs:
                    self.__buffer[slice_start : slice_start + bytes_to_write] = datamv[
                        data_pos : data_pos + bytes_to_write
                    ]
                else:  # rotation
                    slice_end %= self.__bs
                    sep = self.__bs - slice_start
                    self.__buffer[slice_start : self.__bs] = datamv[
                        data_pos : data_pos + sep
                    ]
                    remaining_bytes = bytes_to_write - sep
                    self.__buffer[0:remaining_bytes] = datamv[
                        data_pos + sep : data_pos + bytes_to_write
                    ]
                self.__head += bytes_to_write
                self.__head %= self.__bs
                data_pos += bytes_to_write
                if self.__reading_bytes > 0:
                    if self.__bytes_available() >= self.__reading_bytes:
                        self.__buffer_modify.notify()
        return data_pos

    def close(self):
        """Close stream."""
        self.__closed = True
        with self.__buffer_modify:
            self.__buffer_modify.notify()

    def tell(self) -> int:
        """Get the number of bytes read from the stream.

        :return: number of bytes read from the stream.
        :rtype: int
        """
        return self._bytes_read


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
