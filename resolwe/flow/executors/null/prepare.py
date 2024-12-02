"""Null workflow executor.

To customize the settings the manager serializes for the executor
at runtime, properly subclass
:class:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer` into
``FlowExecutorPreparer`` and override its
:meth:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer.extend_settings`
method.
"""

from resolwe.flow.models import Data, Worker

from ..prepare import BaseFlowExecutorPreparer  # noqa: F401


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the null executor."""

    def prepare_for_execution(self, data):
        """Prepare the data object for the execution.

        Mark data and its worker object as completed.
        """
        data.status = Data.STATUS_DONE
        data.worker.status = Worker.STATUS_COMPLETED
        data.worker.save(update_fields=["status"])
        data.save(update_fields=["status"])
