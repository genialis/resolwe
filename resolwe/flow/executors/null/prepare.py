"""Null workflow executor.

To customize the settings the manager serializes for the executor
at runtime, properly subclass
:class:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer` into
``FlowExecutorPreparer`` and override its
:meth:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer.extend_settings`
method.
"""

from resolwe.flow.models import Worker

from ..prepare import BaseFlowExecutorPreparer  # noqa: F401


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the null executor."""

    def prepare_for_execution(self, data):
        """Prepare the data object for the execution.

        Mark worker object as done.
        """
        data.worker.status = Worker.STATUS_COMPLETED
        data.worker.save()
