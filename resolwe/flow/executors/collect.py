"""Collect files after worker is finished with processing."""
import logging
import os
import shutil
from pathlib import Path
from typing import List, Set, Tuple

from .protocol import ExecutorProtocol

# Make sure sphinx can import this module.
try:
    from .connectors.utils import get_transfer_object
except ImportError:
    pass


logger = logging.getLogger(__name__)


def collect_and_purge(base_path: str, refs: List[str]) -> Tuple[Set[str], Set[str]]:
    """Collect all files and directories that are listed in the list refs."""

    def collect_parent_dirs(base_path: str, path: str) -> List[str]:
        """Also collect parent directories until base_path."""
        base_path = os.path.join(base_path, "")
        parents = []
        path = os.path.dirname(path)
        while path.startswith(base_path):
            parents.append(os.path.join(os.path.relpath(path, base_path), ""))
            path = os.path.dirname(path)
        return parents

    def get_dir_files(base_dir: str, dir_: str) -> Set[str]:
        """Get a list of all elements in the directory.

        Returned paths are relative with respect to the base_dir.
        """
        collected = set()
        for root, _, files in os.walk(dir_):
            collected.add(os.path.join(os.path.relpath(root, base_dir), ""))
            collected.update(
                [
                    os.path.relpath(os.path.join(root, file_), base_dir)
                    for file_ in files
                ]
            )
        return collected

    collected = set()
    removed = set()

    for ref in refs:
        real_path = os.path.join(base_path, ref)
        if not os.path.exists(real_path):
            continue
        collected.update(collect_parent_dirs(base_path, real_path))
        if os.path.isdir(real_path):
            collected.update(get_dir_files(base_path, real_path))
        else:
            collected.add(ref)

    # Remove unreferenced files.
    for root, dirs, files in os.walk(os.path.join(base_path)):
        for file_ in files:
            real_path = os.path.join(root, file_)
            relative_path = os.path.relpath(real_path, base_path)
            if relative_path not in collected:
                os.remove(real_path)
                removed.add(relative_path)
        # Modify dirs in place in order not to visit removed directories.
        i = 0
        while i < len(dirs):
            dir_ = dirs[i]
            real_path = os.path.join(root, dir_)
            relative_path = os.path.join(os.path.relpath(real_path, base_path), "")
            if relative_path not in collected:
                if os.path.isdir(real_path):
                    removed.update(get_dir_files(base_path, real_path))
                    shutil.rmtree(real_path)
                dirs.pop(i)
            else:
                i = i + 1
    return collected, removed


async def collect_files():
    """Collect files produced by the worker.

    Keep only files that are referenced in the data model.
    """
    # Make file importable from outside executor environment
    from .global_settings import DATA, EXECUTOR_SETTINGS
    from .manager_commands import send_manager_command

    logger.debug("Collecting files for data object with id {}".format(DATA["id"]))
    reply = await send_manager_command(ExecutorProtocol.GET_REFERENCED_FILES)
    refs = reply[ExecutorProtocol.REFERENCED_FILES]
    base_dir = EXECUTOR_SETTINGS["DATA_DIR"]
    collected, _ = collect_and_purge(base_dir, refs)

    base_dir = Path(base_dir)
    collected_objects = [
        get_transfer_object(base_dir / object_, base_dir) for object_ in collected
    ]

    await send_manager_command(
        ExecutorProtocol.REFERENCED_FILES,
        extra_fields={ExecutorProtocol.REFERENCED_FILES: collected_objects},
    )
