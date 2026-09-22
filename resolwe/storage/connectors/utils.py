"""Connector utils."""

import asyncio
import concurrent.futures
import os
import threading
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

from .hasher import compute_hashes

if TYPE_CHECKING:
    from os import PathLike

T = TypeVar("T")


def get_transfer_object(
    file_: "PathLike[str]", base_path: "PathLike[str]"
) -> Optional[Dict[str, Union[str, int]]]:
    """Get transfer object from file/directory.

    :param file_: path-like object pointing to a file/directory.

    :param base_path: path in the returned dictionary will be relative with
        respect to this path.

    :returns: dictionary that can be used as an input for transfer methods.
        If file_ argument does not point to a file/directory None is
        returned.
    """

    def get_chunk_size(file_size: int):
        """Chose chunk size for S3.

        The chunk_size must be such that the file_size fits in at most 10_000
        chunks. See
        https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html
        for additional information about this hard limit.

        Min chunk size must be 8 mega bytes. This is also the threshold for
        multipart uploads.
        """
        min_chunk_size = 8 * 1024 * 1024
        needed_chunk_size = int(file_size / 10000) + 1
        return max(min_chunk_size, needed_chunk_size)

    path = Path(file_)
    if not (path.is_file() or path.is_dir()):
        return None

    file_transfer_data = {
        "size": path.stat().st_size,
        "path": os.fspath(path.relative_to(base_path)),
    }
    # Append '/' to directory path.
    if path.is_dir():
        file_transfer_data["path"] = os.path.join(file_transfer_data["path"], "")
        file_transfer_data["size"] = 0
    else:
        file_transfer_data["chunk_size"] = get_chunk_size(file_transfer_data["size"])

    file_transfer_data.update(compute_hashes(path))
    return file_transfer_data


class SharedIterator(Iterator[T]):
    """Thread-safe iterator shared by the worker threads."""

    def __init__(self, iterable: Iterable[T]):
        """Initialize."""
        self._iterator = iter(iterable)
        self._lock = threading.Lock()

    def __next__(self) -> T:
        """Return the next item."""
        with self._lock:
            return next(self._iterator)


def largest_first(objects: Iterable[dict]) -> List[dict]:
    """Sort the transfer objects by size, largest first.

    Started in this order the workers finish at about the same time.
    """
    return sorted(objects, key=lambda entry: entry.get("size", 0), reverse=True)


def paralelize(
    objects: Iterable[Any],
    worker: Callable[[Iterable[Any]], Any],
    max_threads: int = 10,
    abort: Optional[Callable[[], None]] = None,
) -> List[Any]:
    """Run the worker in up to max_threads threads over the objects.

    The workers share one iterator over the objects and each takes the next
    object when it finishes the previous one, so the load balances between
    them. Pass the objects in the order they should start, see
    :func:`largest_first`. Connectors in worker method MUST be duplicated to
    ensure thread safety.

    When a worker fails, ``abort`` is called so the other workers can stop
    early. The function returns or raises once every worker has finished.

    :param abort: called once when the first worker fails.

    :raises Exception: the exception of the first failed worker.

    :returns: the result of each worker thread.
    """
    objects = list(objects)
    if not objects:
        return []
    workers = min(len(objects), max_threads)
    shared = SharedIterator(objects)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, shared) for _ in range(workers)]
        done, _ = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_EXCEPTION
        )
        failed = [future for future in done if future.exception() is not None]
        if failed and abort is not None:
            abort()
    if failed:
        failed[0].result()
    return [future.result() for future in futures]


async def async_paralelize(
    objects: Iterable[Any],
    worker: Callable[[Iterable[Any]], Any],
    loop: asyncio.BaseEventLoop,
    max_threads: int = 10,
    return_when: str = asyncio.ALL_COMPLETED,
) -> List[asyncio.Future]:
    """Run the worker in up to max_threads threads over the objects.

    The workers share the objects as in :func:`paralelize`.

    When the coroutine is cancelled the running workers are left to finish on
    their own: they cannot be interrupted and waiting for them would block the
    event loop.

    :param return_when: when to return, see :func:`asyncio.wait`. The tasks
        not done on return are still running.

    :returns: one Future per worker thread. It is caller responsibility to
        check if threads have finished successfully.
    """
    objects = list(objects)
    if not objects:
        return []
    workers = min(len(objects), max_threads)
    shared = SharedIterator(objects)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    tasks = [loop.run_in_executor(executor, worker, shared) for _ in range(workers)]
    try:
        await asyncio.wait(tasks, return_when=return_when)
    finally:
        executor.shutdown(wait=False)
    return tasks
