"""Global settings for Flow executor."""
import json
import os
import sys

from .protocol import ExecutorFiles

DESERIALIZED_FILES = {}

if "sphinx" not in sys.modules:
    with open(
        os.path.join(ExecutorFiles.SETTINGS_SUBDIR, ExecutorFiles.EXECUTOR_SETTINGS),
        "rt",
    ) as _settings_file:
        DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS] = json.load(_settings_file)
        for _file_name in DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS][
            ExecutorFiles.FILE_LIST_KEY
        ]:
            with open(
                os.path.join(ExecutorFiles.SETTINGS_SUBDIR, _file_name), "rt"
            ) as _json_file:
                DESERIALIZED_FILES[_file_name] = json.load(_json_file)
else:
    DESERIALIZED_FILES = {
        getattr(ExecutorFiles, f): {} for f in dir(ExecutorFiles) if f == f.upper()
    }

EXECUTOR_SETTINGS = DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS]
SETTINGS = DESERIALIZED_FILES[ExecutorFiles.DJANGO_SETTINGS]
DATA = DESERIALIZED_FILES[ExecutorFiles.DATA]
STORAGE_LOCATION = DESERIALIZED_FILES[ExecutorFiles.STORAGE_LOCATION]
DATA_META = DESERIALIZED_FILES[ExecutorFiles.DATA_META]
PROCESS = DESERIALIZED_FILES[ExecutorFiles.PROCESS]
PROCESS_META = DESERIALIZED_FILES[ExecutorFiles.PROCESS_META]
