"""Constants for the executors."""
from pathlib import Path

DATA_LOCAL_VOLUME = Path("/data_local")
DATA_VOLUME = Path("/data")
DATA_ALL_VOLUME = Path("/data_all")
UPLOAD_VOLUME = Path("/upload")
SECRETS_VOLUME = Path("/secrets")
SETTINGS_VOLUME = Path("/settings")
SOCKETS_VOLUME = Path("/sockets")

CONTAINER_TIMEOUT = 300
# Where socket files are stored inside container.
COMMUNICATION_PROCESSING_SOCKET = "_socket1.s"
SCRIPT_SOCKET = "_socket2.s"

TMPDIR = ".tmp"

# Container image used if no image is specified.
DEFAULT_CONTAINER_IMAGE = "resolwe/base:ubuntu-20.04"
