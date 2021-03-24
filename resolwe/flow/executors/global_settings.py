"""Global settings for Flow executor."""
from pathlib import Path
from typing import Any, Dict

from .protocol import ExecutorFiles

DESERIALIZED_FILES: Dict[str, Any] = {
    getattr(ExecutorFiles, f): {} for f in dir(ExecutorFiles) if f == f.upper()
}


def initialize_constants(data_id: int, bootstrap_data: Dict):
    """Update the constants with the bootstrap data."""
    global DATA_ID, EXECUTOR_SETTINGS, SETTINGS, LOCATION_SUBPATH, PROCESS, PROCESS_META
    EXECUTOR_SETTINGS = bootstrap_data.get(ExecutorFiles.EXECUTOR_SETTINGS)
    SETTINGS = bootstrap_data.get(ExecutorFiles.DJANGO_SETTINGS)
    LOCATION_SUBPATH = Path(bootstrap_data.get(ExecutorFiles.LOCATION_SUBPATH, "-1"))
    PROCESS = bootstrap_data.get(ExecutorFiles.PROCESS)
    PROCESS_META = bootstrap_data.get(ExecutorFiles.PROCESS_META)
    DATA_ID = data_id


EXECUTOR_SETTINGS = DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS]
SETTINGS = DESERIALIZED_FILES[ExecutorFiles.DJANGO_SETTINGS]
LOCATION_SUBPATH = DESERIALIZED_FILES[ExecutorFiles.LOCATION_SUBPATH]
PROCESS = DESERIALIZED_FILES[ExecutorFiles.PROCESS]
PROCESS_META = DESERIALIZED_FILES[ExecutorFiles.PROCESS_META]
DATA_ID = -1
