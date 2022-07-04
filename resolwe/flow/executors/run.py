""".. Ignore pydocstyle D400.

==========
Base Class
==========

.. autoclass:: resolwe.flow.executors.run.BaseFlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

from .global_settings import LOCATION_SUBPATH, PROCESS, SETTINGS
from .zeromq_utils import ZMQCommunicator

# TODO: update requirements!!!!
# NOTE: If the imports here are changed, the executors' requirements.txt
# file must also be updated accordingly.
logger = logging.getLogger(__name__)


class BaseFlowExecutor:
    """Represents a workflow executor."""

    # Name has to be overriden in inherited classes.
    name = "base"

    def __init__(
        self,
        data_id: int,
        communicator: ZMQCommunicator,
        listener_connection: Tuple[str, str, str],
        *args,
        **kwargs,
    ):
        """Initialize attributes.

        :attr data_id: id of the Data object we are processing.
        :attr communicator: the communicator object for communication with the listener.
        :attr listener_connection: tuple (host, port, protocol) determining the address
            of the listener.
        """
        self.data_id = data_id
        self.process: Dict = PROCESS
        process_requirements: Dict = self.process.get("requirements", {})
        self.requirements: Dict = process_requirements.get("executor", {}).get(
            self.name, {}
        )
        self.resources: Dict = process_requirements.get("resources", {})
        self.storage_url = Path(LOCATION_SUBPATH)
        self.runtime_dir = Path(SETTINGS["FLOW_VOLUMES"]["runtime"]["config"]["path"])
        self.communicator = communicator
        self.listener_connection = list(listener_connection)
        if not sys.platform.startswith("linux"):
            self.listener_connection[0] = "host.docker.internal"
        self.tools_paths_prefix = Path("/usr/local/bin/resolwe")

    def _generate_container_name(self, prefix: str) -> str:
        """Generate unique container name."""
        return "{}-{}".format(prefix, self.data_id)

    def get_tools_paths(self):
        """Get tools paths."""
        return SETTINGS["FLOW_EXECUTOR_TOOLS_PATHS"]

    async def start(self):
        """Start process execution."""

    async def run(self):
        """Execute the script and save results."""
        logger.debug("Executor for Data with id %d has started.", self.data_id)
        await self.start()
