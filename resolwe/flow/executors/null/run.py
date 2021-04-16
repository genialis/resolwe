""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.null.run.FlowExecutor

"""
import logging

from ..run import BaseFlowExecutor

logger = logging.getLogger(__name__)


class FlowExecutor(BaseFlowExecutor):
    """Null dataflow executor proxy.

    This executor is intended to be used in tests where you want to save
    the object to the database but don't need to run it.
    """

    name = "null"

    async def run(self):
        """Do nothing :)."""
        pass
