"""Settings for storage app.

Used to provide s simple default configuration.
"""
from pathlib import Path

from django.conf import settings

project_root = Path(getattr(settings, "PROJECT_ROOT", "/"))
local_connector = "local"
local_dir = project_root / ".test_data"

upload_connector = "upload"
upload_dir = project_root / ".test_upload"

default_storage_connectors = {
    local_connector: {
        "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
        "config": {
            "priority": 100,
            "path": local_dir,
            "selinux_label": "z",
        },
    },
    upload_connector: {
        "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
        "config": {
            "priority": 100,
            "path": upload_dir,
            "selinux_label": "z",
        },
    },
}

STORAGE_CONNECTORS = getattr(settings, "STORAGE_CONNECTORS", default_storage_connectors)

default_storages = {
    "data": {"connectors": ["local"]},
    "upload": {"connectors": ["upload"]},
}

FLOW_STORAGE = getattr(settings, "FLOW_STORAGE", default_storages)

# This entry must contain the key 'path' inside 'config' dictionary. The value
# is the path where runtime volume is accesible at the host that is executing
# the code (main server for kubernetes, worker nodes for celery/slurm ...).
# Types 'host_path' and 'persistent_volume' are supported.
default_runtime_volume = {
    "type": "host_path",
    "config": {
        "path": project_root / ".test_runtime",
        "name": "runtime",
        "read_only": True,
    },
}

default_processing_volume = {
    "type": "host_path",
    "config": {
        "path": project_root / ".test_processing",
        "name": "processing",
    },
}

default_volumes = {
    "processing": default_processing_volume,
    "runtime": default_runtime_volume,
}

FLOW_VOLUMES = getattr(settings, "FLOW_VOLUMES", default_volumes)
