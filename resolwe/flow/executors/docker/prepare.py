"""Docker workflow executor, preparation portion.

To customize the settings the manager serializes for the executor
at runtime, properly subclass
:class:`~resolwe.flow.executors.prepare.BaseFlowExecutorPreparer` into
``FlowExecutorPreparer`` and override its :meth:`extend_settings`
method.
"""

from django.conf import settings
from django.core.management import call_command

from ..prepare import BaseFlowExecutorPreparer


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the docker executor."""

    def post_register_hook(self):
        """Pull Docker images needed by processes after registering."""
        if not getattr(settings, 'FLOW_DOCKER_DONT_PULL', False):
            call_command('list_docker_images', pull=True)
