"""Connector utils."""
import asyncio
import concurrent.futures
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
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


def chunks(iterable: Sequence[T], n: int) -> Iterable[Sequence[T]]:
    """Split iterable into n evenly sized chunks."""
    for i in range(0, n):
        yield iterable[i::n]


def paralelize(
    objects: Sequence[Any],
    worker: Callable[[Sequence[Any]], Any],
    max_threads: int = 10,
) -> Sequence[concurrent.futures.Future]:
    """Paralelize tasks using connector on list of URLS.

    URLs are split into up-to num_threads chunks and each chunk is processed
    in its own thread. Connectors in worker method MUST be duplicated to ensure
    thread safety.

    :returns: collection of instance of Future objects, each one corresponding
        to one thread. It is caller responsibility to check if threads have
        finished successfully.
    """
    number_of_chunks = min(len(objects), max_threads)
    objects_chunks = chunks(objects, number_of_chunks)

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for objects_chunk in objects_chunks:
            futures.append(executor.submit(worker, objects_chunk))
    return futures


async def async_paralelize(
    objects: Sequence[Any],
    worker: Callable[[Sequence[Any]], Any],
    loop: asyncio.BaseEventLoop,
    max_threads: int = 10,
) -> Sequence[asyncio.Future]:
    """Paralelize tasks using connector on list of URLS.

    URLs are split into up-to num_threads chunks and each chunk is processed
    in its own thread. Connectors in worker method MUST be duplicated to ensure
    thread safety.

    :returns: collection of instance of Future objects, each one corresponding
        to one thread. It is caller responsibility to check if threads have
        finished successfully.
    """
    number_of_chunks = min(len(objects), max_threads)
    objects_chunks = chunks(objects, number_of_chunks)

    tasks = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        for objects_chunk in objects_chunks:
            tasks.append(loop.run_in_executor(executor, worker, objects_chunk))
        await asyncio.wait(tasks)
    return tasks
