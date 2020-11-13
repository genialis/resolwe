"""Collect files after worker is finished with processing.

Since only files referenced in output are copied here there is nothing to do.
Just store the list of all files and send it to the listener.
"""
import asyncio
import concurrent
import logging
import os
from pathlib import Path
from typing import Set

from . import constants
from .socket_utils import BaseCommunicator, Message

# Make sure sphinx can import this module.
try:
    from .connectors.utils import get_transfer_object
except ImportError:
    pass


DATA_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data"))

logger = logging.getLogger(__name__)


def collect(base_path: str) -> Set[str]:
    """Collect all files in the given directory."""

    collected = set()
    for root, _, files in os.walk(base_path):
        if root != base_path:
            collected.add(os.path.join(os.path.relpath(root, base_path), ""))
        collected.update(
            (os.path.relpath(os.path.join(root, file_), base_path) for file_ in files)
        )
    return collected


def hydrate_size(output_files_dirs: dict, base_path: Path):
    """Update output fields to include size and total_size of referenced files.

    Add size and total_size to ``basic:file:``, ``list:basic:file``,
    ``basic:dir:`` and ``list:basic:dir:`` fields.
    """

    def get_dir_size(path):
        """Get directory size."""
        return sum(
            file_.stat().st_size for file_ in Path(path).rglob("*") if file_.is_file()
        )

    def get_refs_size(obj, obj_path):
        """Calculate size of all references of ``obj``.

        :param dict obj: Data object's output field (of type file/dir).
        :param Path obj_path: Path to ``obj``.
        """
        total_size = 0
        for ref in obj.get("refs", []):
            ref_path = base_path / ref
            if ref_path < obj_path:
                # It is a common case that ``obj['file']`` is also contained in
                # one of obj['ref']. In that case, we need to make sure that it's
                # size is not counted twice:
                continue
            if ref_path.is_file():
                total_size += ref_path.stat().st_size
            elif ref_path.is_dir():
                total_size += get_dir_size(ref_path)

        return total_size

    def add_file_size(obj):
        """Add file size to the basic:file field."""
        path = base_path / obj["file"]
        if not path.is_file():
            # TODO: this was validation error before.
            raise RuntimeError("Referenced file does not exist ({})".format(path))

        obj["size"] = path.stat().st_size
        obj["total_size"] = obj["size"] + get_refs_size(obj, path)

    def add_dir_size(obj):
        """Add directory size to the basic:dir field."""
        path = base_path / obj["dir"]
        if not path.is_dir():
            # TODO: this was validation error before.
            raise RuntimeError("Referenced dir does not exist ({})".format(path))

        obj["size"] = get_dir_size(path)
        obj["total_size"] = obj["size"] + get_refs_size(obj, path)

    data_size = 0
    output = dict()
    for field_name, (field_type, field) in output_files_dirs.items():
        # Ignore fields not in schema. Validation wil be performed by the
        # listener at the end of the process.
        if field_type.startswith("basic:file:"):
            add_file_size(field)
            data_size += field.get("total_size", 0)
        elif field_type.startswith("list:basic:file:"):
            for obj in field:
                add_file_size(obj)
                data_size += obj.get("total_size", 0)
        elif field_type.startswith("basic:dir:"):
            add_dir_size(field)
            data_size += field.get("total_size", 0)
        elif field_type.startswith("list:basic:dir:"):
            for obj in field:
                add_dir_size(obj)
                data_size += obj.get("total_size", 0)
        output[field_name] = field

    return output, data_size


async def collect_files(communicator: BaseCommunicator, keep_data=False):
    """Collect files produced by the worker.

    They are neatly prepared in the DATA_VOLUME.

    The time consuming part must be run in a thread or it will block the
    entire comminication container startup script, creating heartbeat timeouts
    and therefore failures.

    :raises RuntimeError: on failure.
    """
    loop = asyncio.get_event_loop()

    try:
        base_dir = constants.DATA_VOLUME
        with concurrent.futures.ThreadPoolExecutor() as pool:
            collected = await loop.run_in_executor(pool, collect, base_dir)

        collected_objects = [
            get_transfer_object(base_dir / object_, base_dir) for object_ in collected
        ]

        await communicator.send_command(
            Message.command("referenced_files", collected_objects)
        )

        # Update output sizes and entire data object size.
        response = await communicator.send_command(
            Message.command("get_output_files_dirs", "")
        )

        with concurrent.futures.ThreadPoolExecutor() as pool:
            output, data_size = await loop.run_in_executor(
                pool, hydrate_size, response.message_data, base_dir
            )

        if output:
            await communicator.send_command(Message.command("update_output", output))
            await communicator.send_command(Message.command("set_data_size", data_size))

    except Exception as ex:
        logger.exception("Error collecting files")
        raise RuntimeError(f"Error collection files: {ex}")
