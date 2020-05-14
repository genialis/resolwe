"""Connector utils."""
import os
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional, Union

from .hasher import compute_hashes

if TYPE_CHECKING:
    from os import PathLike


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
    file_transfer_data.update(compute_hashes(path))
    return file_transfer_data
