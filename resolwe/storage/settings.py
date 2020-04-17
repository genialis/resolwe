"""Settings for storage app.

Used to provide s simple default configuration.
"""
from django.conf import settings

data_dir = getattr(settings, "FLOW_EXECUTOR", {}.get("DATA_DIR", "/some_path"))
default_local_connector = "local"
default_storage_connectors = {
    "local": {
        "connector": "resolwe.storage.connectors.localconnector.LocalFilesystemConnector",
        "config": {"priority": 0, "path": data_dir},
    },
}

LOCAL_CONNECTOR = getattr(settings, "LOCAL_CONNECTOR", default_local_connector)
STORAGE_CONNECTORS = getattr(settings, "STORAGE_CONNECTORS", default_storage_connectors)
