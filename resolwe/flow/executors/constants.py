"""Constants for the executors."""
from pathlib import Path

SECRETS_VOLUME = Path("/secrets")
SOCKETS_VOLUME = Path("/sockets")
INPUTS_VOLUME = Path("/inputs")
PROCESSING_VOLUME = Path("/processing")

CONTAINER_TIMEOUT = 600

# Relative path to socket files inside sockets volume.
COMMUNICATION_PROCESSING_SOCKET = "_socket1.s"
SCRIPT_SOCKET = "_socket2.s"
UPLOAD_FILE_SOCKET = "_upload_socket.s"

TMPDIR = ".tmp"

INPUTS_VOLUME_NAME = "input"
PROCESSING_VOLUME_NAME = "processing"
RUNTIME_VOLUME_NAME = "runtime"
SECRETS_VOLUME_NAME = "secrets"
SOCKETS_VOLUME_NAME = "sockets"

BOOTSTRAP_PYTHON_RUNTIME = "bootstrap_python_runtime.py"
