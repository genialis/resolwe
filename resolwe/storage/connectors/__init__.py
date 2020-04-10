""".. Ignore pydocstyle D400.

==========================
Resolwe Storage Connectors
==========================

"""

from .baseconnector import DEFAULT_CONNECTOR_PRIORITY
from .localconnector import LocalFilesystemConnector
from .registry import StorageConnectors, connectors
from .transfer import Transfer

try:
    from .s3connector import AwsS3Connector
except ImportError:
    AwsS3Connector = None
try:
    from .googleconnector import GoogleConnector
except ImportError:
    GoogleConnector = None


__all__ = (
    "AwsS3Connector",
    "GoogleConnector",
    "LocalFilesystemConnector",
    "StorageConnectors",
    "Transfer",
    "DEFAULT_CONNECTOR_PRIORITY",
    "connectors",
)
