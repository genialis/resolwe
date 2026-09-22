"""Data transfer between connectors."""

import concurrent.futures
import logging
import threading
from contextlib import contextmanager, suppress
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, List, Union

import wrapt
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout
from urllib3.exceptions import ProtocolError

from .baseconnector import BaseStorageConnector
from .circular_buffer import CircularBuffer
from .exceptions import DataTransferError
from .utils import largest_first, paralelize

if TYPE_CHECKING:
    from os import PathLike

try:
    from google.api_core.exceptions import ServerError, TooManyRequests
    from google.resumable_media.common import DataCorruption

    gcs_exceptions = [DataCorruption, ServerError, TooManyRequests]
except ModuleNotFoundError:
    gcs_exceptions = []

try:
    from botocore.exceptions import ClientError
    from botocore.exceptions import ConnectionError as BotoConnectionError
    from botocore.exceptions import HTTPClientError, IncompleteReadError
    from s3transfer.exceptions import RetriesExceededError

    # download_fileobj retries the read errors of the object body itself and
    # then raises RetriesExceededError, not the read error.
    boto_exceptions = [
        ClientError,
        BotoConnectionError,
        HTTPClientError,
        IncompleteReadError,
        RetriesExceededError,
    ]
    client_errors: tuple = (ClientError,)
except ModuleNotFoundError:
    boto_exceptions = []
    client_errors = ()


logger = logging.getLogger(__name__)
# Attempts with 1, 2, ..., 32, 60 seconds between them: about two minutes.
ERROR_MAX_RETRIES = 8
ERROR_INITIAL_TIMEOUT = 1  # In seconds.
ERROR_MAX_TIMEOUT = 60  # In seconds.
transfer_exceptions = tuple(
    boto_exceptions
    + gcs_exceptions
    + [RequestsConnectionError, ReadTimeout, ConnectionResetError, ProtocolError]
    + [DataTransferError]
)


# The 400 error code of S3 for a request it did not read in time.
RETRYABLE_CLIENT_ERROR_CODES = {"RequestTimeout"}


def is_transient(exception: Exception) -> bool:
    """Return True when the failed request can be repeated.

    A botocore ClientError is transient only for a server error (5xx), a
    throttling response (429) or a request timeout. The other client errors,
    such as a missing object or denied access, do not change on a retry. A
    DataTransferError raised from another error is classified by that error.
    """
    if isinstance(exception, DataTransferError) and exception.__cause__ is not None:
        return is_transient(exception.__cause__)
    if not isinstance(exception, client_errors):
        return True
    response = exception.response
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    code = response.get("Error", {}).get("Code", "")
    return status >= 500 or status == 429 or code in RETRYABLE_CLIENT_ERROR_CODES


@wrapt.decorator
def retry_on_transfer_error(wrapped, instance, args, kwargs):
    """Retry a transient transfer error with exponential backoff.

    The wrapped function must be a method of :class:`Transfer`. The call is
    not retried when the error is not transient (see
    :func:`is_transient`) or when the transfer instance is aborted. The wait
    between the attempts ends on abort. The last failure is raised.
    """
    for retry in range(1, ERROR_MAX_RETRIES + 1):
        try:
            return wrapped(*args, **kwargs)
        except transfer_exceptions as exception:
            if (
                retry == ERROR_MAX_RETRIES
                or not is_transient(exception)
                or instance.aborted
            ):
                raise
            timeout = min(ERROR_MAX_TIMEOUT, ERROR_INITIAL_TIMEOUT * (2 ** (retry - 1)))
            logger.warning(
                "Attempt %d/%d failed with %r, retrying in %d seconds.",
                retry,
                ERROR_MAX_RETRIES,
                exception,
                timeout,
            )
            if instance.wait_aborted(timeout):
                raise


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
        self._aborted = threading.Event()
        # Streams of the objects in transfer, closed when the transfer is aborted.
        self._streams: set = set()
        self._streams_lock = threading.Lock()

    @property
    def aborted(self) -> bool:
        """Get True if the transfer is aborted."""
        return self._aborted.is_set()

    def wait_aborted(self, timeout: float) -> bool:
        """Wait at most timeout seconds for the abort, return True on abort."""
        return self._aborted.wait(timeout)

    def abort(self):
        """Abort the transfer.

        The worker threads stop before the next object. The streams of the
        objects in transfer are closed so their transfer fails at once. The
        method can be called from any thread and returns immediately.
        """
        self._aborted.set()
        with self._streams_lock:
            for stream in self._streams:
                with suppress(Exception):
                    stream.close()

    @contextmanager
    def _abortable(self, stream) -> Iterator:
        """Close the stream on exit and when the transfer is aborted."""
        with self._streams_lock:
            self._streams.add(stream)
            if self.aborted:
                stream.close()
        try:
            yield stream
        finally:
            with self._streams_lock:
                self._streams.discard(stream)
            stream.close()

    def pre_processing(self, url: Union[str, Path], objects: List[dict]):
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

    def post_processing(self, url: Union[str, Path], objects: List[dict]):
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
    ) -> List[dict]:
        """Transfer objects under the given URL.

        Objects are read from from_connector and copied to to_connector.

        :param url: the given URL to transfer from/to.

        :param objects: the list of objects to transfer. Each object is
            represented with the dictionary containing at least keys "path",
            "size", "md5", "crc32c", "awss3etag", "chunk_size".
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

        # Raises the exception of the first failed worker after the others
        # are aborted.
        transfered = paralelize(
            objects=largest_first(objects_to_transfer),
            worker=partial(self.transfer_chunk, url),
            max_threads=max_threads,
            abort=self.abort,
        )
        if not all(transfered):
            raise DataTransferError()

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

        return objects_stored

    def transfer_chunk(self, url: Path, objects: Iterable[dict]) -> bool:
        """Transfer a single chunk of objects.

        When objects have properties `from_base_url` and `to_base_url` they
        override the `url` argument.

        :raises DataTransferError: on failure or when the transfer is aborted.
        :returns: True on success.
        """
        to_connector = self.to_connector.duplicate()
        from_connector = self.from_connector.duplicate()
        for entry in objects:
            # Do not transfer directories.
            if entry["path"].endswith("/"):
                continue
            if self.aborted:
                raise DataTransferError("Transfer aborted.")
            from_base_url = entry.get("from_base_url", url)
            to_base_url = entry.get("to_base_url", url)
            to_url = Path(entry["path"])
            from_url = Path(from_base_url) / to_url
            try:
                if not self.transfer(
                    from_base_url,
                    entry,
                    to_base_url,
                    to_url,
                    from_connector,
                    to_connector,
                ):
                    raise DataTransferError(f"Transfer of '{from_url}' failed")
            except DataTransferError:
                # The aborted transfer fails on its closed stream. A
                # DataTransferError already names the object.
                if not self.aborted:
                    raise
            except Exception as exception:
                if not self.aborted:
                    raise DataTransferError(
                        f"Transfer of '{from_url}' failed: {exception!r}"
                    ) from exception
            if self.aborted:
                # Remove the partially transfered object.
                with suppress(Exception):
                    to_connector.delete(to_base_url, [to_url])
                raise DataTransferError("Transfer aborted.")
        return True

    @retry_on_transfer_error
    def transfer(
        self,
        from_base_url: Union[str, Path],
        object_: dict,
        to_base_url: Union[str, Path],
        to_url: "PathLike[str]",
        from_connector: "BaseStorageConnector" = None,
        to_connector: "BaseStorageConnector" = None,
    ) -> bool:
        """Transfer single object between two storage connectors.

        :param from_base_url: base url on from_connector.

        :param object_: object to transfer. It must be a dictionary containing
            at least keys "path", "md5", "crc32c", "size" and "awss3etag". It
            can also contain key "chunk_size" that specifies a custom
            chunk_size to use for upload / download.

        :param to_base_url: base url on to_connector.

        :param to_url: where to copy object. It is relative with respect to the
            argument to_base_url.

        :param from_connector: from connector, defaults to None. If None
            duplicate of from_connector from the Transfer class instance is
            used.

        :param to_connector: to connector, defaults to None. If None
            duplicate of to_connector from the Transfer class instance is
            used.

        :raises DataTransferError: on failure.

        :returns: True on success.
        """
        to_base_url = Path(to_base_url)
        chunk_size = object_.get("chunk_size", BaseStorageConnector.CHUNK_SIZE)
        # Duplicate connectors for thread safety.
        to_connector = to_connector or self.to_connector.duplicate()
        from_connector = from_connector or self.from_connector.duplicate()

        from_url = Path(from_base_url) / object_["path"]
        hashes = {type_: object_[type_] for type_ in ["md5", "crc32c", "awss3etag"]}

        skip_final_hash_check = (
            from_connector.get_ensures_data_integrity
            and to_connector.put_ensures_data_integrity
        )
        if skip_final_hash_check:
            # When final check is skipped make sure that the input connector
            # hash equals to the hash given by the _object (usually read from
            # the database). This ensures that the data was not modified.
            hash_to_check = next(
                hash for hash in from_connector.supported_hash if hash in hashes.keys()
            )
            from_connector_hash = from_connector.get_hash(from_url, hash_to_check)
            expected_hash = object_[hash_to_check]
            if expected_hash != from_connector_hash:
                raise DataTransferError(
                    f"Connector {from_connector} has {from_connector_hash} stored  "
                    f"as {from_connector_hash} hash for object "
                    f"{from_url}, expected {expected_hash}."
                )

        common_hash_type = next(
            e for e in to_connector.supported_hash if e in hashes.keys()
        )
        from_hash = hashes[common_hash_type]

        # Check if file with the correct hash already exist in to_connector.
        to_hash = to_connector.get_hash(to_base_url / to_url, common_hash_type)
        if from_hash == to_hash:
            logger.debug(
                "From: {}:{}".format(from_connector.name, from_url)
                + " to: {}:{}".format(to_connector.name, to_base_url / to_url)
                + " object exists with right hash, skipping."
            )
            return True

        # We have three posible ways of transfering the data:
        # - if from_connector supports streams then we open the stream and
        #   transfer the data.
        # - if to_connector supporst streams then we transfer the data from
        #   from_connector directly to the opened stream.
        # - if neither support streams then we use buffer to transfer the data
        #   from from_connector to to_connector.
        try:
            if from_connector.can_open_stream:
                with self._abortable(
                    from_connector.open_stream(from_url, "rb")
                ) as stream:
                    to_connector.push(
                        stream,
                        to_base_url / to_url,
                        chunk_size=chunk_size,
                        hashes=hashes,
                    )

            elif to_connector.can_open_stream:
                with self._abortable(
                    to_connector.open_stream(to_base_url / to_url, "wb")
                ) as stream:
                    from_connector.get(from_url, stream, chunk_size=chunk_size)
                to_connector.set_hashes(to_base_url / to_url, hashes)

            # Otherwise create out own stream and use threads to transfer data.
            else:

                def future_done(stream_to_close, future):
                    stream_to_close.close()
                    if future.exception() is not None:
                        executor.shutdown(wait=False)

                # The constant 1 is added to the object size to make sure the
                # buffer_size is at least 1. Otherwise uploading files of size 0
                # will cause a deadlock.
                data_stream = CircularBuffer(
                    buffer_size=min(200 * 1024 * 1024, object_["size"] + 1)
                )
                with (
                    self._abortable(data_stream),
                    concurrent.futures.ThreadPoolExecutor() as executor,
                ):
                    download_task = executor.submit(
                        from_connector.get,
                        from_url,
                        data_stream,
                        chunk_size=chunk_size,
                    )
                    upload_task = executor.submit(
                        to_connector.push,
                        data_stream,
                        to_base_url / to_url,
                        chunk_size=chunk_size,
                        hashes=hashes,
                    )
                    download_task.add_done_callback(partial(future_done, data_stream))
                    futures = (download_task, upload_task)

                # If any of the transfer futures has raised an exception the
                # upload must be aborted and DataTransferError raised.
                #
                # When transfer was a success we have to store the server-side
                # computed hashes to the hashes given on object. They may differ
                # from computed ones since server-side encryption can be used
                # which changes hashes (at least this happens with awss3etag when
                # SSE-KMS encryption is used on S3 bucket).
                if any(f.exception() is not None for f in futures):
                    # Log exceptions in threads to preserve original stack trace.
                    for f in futures:
                        try:
                            f.result()
                        except Exception:
                            logger.exception("Exception occured while transfering data")

                    # Re-raise exception. Chain the first error, so the retry
                    # decorator can tell whether it is transient.
                    ex = [f.exception() for f in futures if f.exception() is not None]
                    raise DataTransferError(
                        f"Transfer of '{from_url}' failed: "
                        + ", ".join(repr(e) for e in ex)
                    ) from ex[0]
        except Exception:
            # Remove the partial object so the retry does not hash it.
            with suppress(Exception):
                to_connector.delete(to_base_url, [to_url])
            raise

        # Check hash of the uploaded object.
        if not skip_final_hash_check:
            to_hash = to_connector.get_hash(to_base_url / to_url, common_hash_type)
            if from_hash != to_hash:
                with suppress(Exception):
                    to_connector.delete(to_base_url, [to_url])
                raise DataTransferError(
                    f"Hash {common_hash_type} does not match while transfering "
                    f"{from_url} -> {to_base_url/to_url}: using hash type "
                    f"{common_hash_type}: expected {from_hash}, got {to_hash}."
                )

        for hash_type in to_connector.refresh_hash_after_transfer:
            hash = to_connector.get_hash(to_base_url / to_url, hash_type)
            object_[hash_type] = hash

        return True
