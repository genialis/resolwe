""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.null.run.FlowExecutor

"""
import logging

from ..manager_commands import send_manager_command
from ..protocol import ExecutorProtocol
from ..run import BaseFlowExecutor

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class FlowExecutor(BaseFlowExecutor):  # pylint: disable=abstract-method
    """Null dataflow executor proxy.

    This executor is intended to be used in tests where you want to save
    the object to the database but don't need to run it.
    """

    name = 'null'

    async def run(self, data_id, script):
        """Do nothing :)."""
        await send_manager_command(ExecutorProtocol.FINISH)
