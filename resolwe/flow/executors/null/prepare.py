"""Null workflow executor.

To customize the settings the manager serializes for the executor
at runtime, properly subclass
:class:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer` into
``FlowExecutorPreparer`` and override its
:meth:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer.extend_settings`
method.
"""
from ..prepare import BaseFlowExecutorPreparer  # noqa: F401


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the null executor."""
