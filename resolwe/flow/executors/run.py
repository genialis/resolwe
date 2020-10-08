""".. Ignore pydocstyle D400.

==========
Base Class
==========

.. autoclass:: resolwe.flow.executors.run.BaseFlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import logging
import os
from typing import Dict

from .global_settings import EXECUTOR_SETTINGS, PROCESS, SETTINGS

# TODO: update requirements!!!!
# NOTE: If the imports here are changed, the executors' requirements.txt
# file must also be updated accordingly.
logger = logging.getLogger(__name__)


class BaseFlowExecutor:
    """Represents a workflow executor."""

    # Name has to be overriden in inherited classes.
    name = "base"

    def __init__(self, data_id: int, *args, **kwargs):
        """Initialize attributes."""
        # This overrides name set in executor!!!
        # self.name: Optional[str] = None
        self.data_id = data_id
        self.process: Dict = PROCESS
        process_requirements: Dict = self.process.get("requirements", {})
        self.requirements: Dict = process_requirements.get("executor", {}).get(
            self.name, {}
        )
        self.resources: Dict = process_requirements.get("resources", {})

    def _generate_container_name(self, prefix):
        """Generate unique container name.

        Name of the kubernetes container should contain only lower case
        alpfanumeric characters and dashes. Underscores are not allowed.
        """
        return "{}-{}".format(prefix, self.data_id)

    def get_tools_paths(self):
        """Get tools paths."""
        tools_paths = SETTINGS["FLOW_EXECUTOR_TOOLS_PATHS"]
        return tools_paths

    async def start(self):
        """Start process execution."""

    async def run(self):
        """Execute the script and save results."""
        logger.debug("Executor for Data with id %d has started.", self.data_id)
        try:
            os.chdir(EXECUTOR_SETTINGS["DATA_DIR"])
            await self.start()
        except:
            logger.exception("Unhandled exception in executor")
            raise
