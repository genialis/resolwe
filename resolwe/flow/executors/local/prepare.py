""".. Ignore pydocstyle D400.

===========
Preparation
===========

.. autoclass:: resolwe.flow.executors.local.prepare.FlowExecutorPreparer
    :members:

"""

from django.core.exceptions import PermissionDenied

from resolwe.flow.models import Data

from ..prepare import BaseFlowExecutorPreparer


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the local executor."""

    def extend_settings(self, data_id, files, secrets):
        """Prevent processes requiring access to secrets from being run."""
        process = Data.objects.get(pk=data_id).process
        if process.requirements.get('resources', {}).get('secrets', False):
            raise PermissionDenied(
                "Process which requires access to secrets cannot be run using the local executor"
            )

        return super().extend_settings(data_id, files, secrets)
