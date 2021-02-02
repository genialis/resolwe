"""Constants for the executors."""
from pathlib import Path

DATA_LOCAL_VOLUME = Path("/data_local")
DATA_VOLUME = Path("/data")
DATA_ALL_VOLUME = Path("/data_all")
UPLOAD_VOLUME = Path("/upload")
SECRETS_VOLUME = Path("/secrets")
SETTINGS_VOLUME = Path("/settings")
SOCKETS_VOLUME = Path("/sockets")
INPUTS_VOLUME = Path("/inputs")

CONTAINER_TIMEOUT = 600
# Where socket files are stored inside container.
COMMUNICATION_PROCESSING_SOCKET = "_socket1.s"
SCRIPT_SOCKET = "_socket2.s"
# Used to upload files from processing to communication container.
UPLOAD_FILE_SOCKET = "_upload_socket.s"

TMPDIR = ".tmp"
