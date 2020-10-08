""".. Ignore pydocstyle D400.

===========
Preparation
===========

.. autoclass:: resolwe.flow.executors.docker.prepare.FlowExecutorPreparer
    :members:

"""
import os

from django.conf import settings
from django.core.management import call_command

from .. import constants
from ..prepare import BaseFlowExecutorPreparer


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the docker executor."""

    def post_register_hook(self, verbosity=1):
        """Pull Docker images needed by processes after registering."""
        if not getattr(settings, "FLOW_DOCKER_DONT_PULL", False):
            call_command("list_docker_images", pull=True, verbosity=verbosity)

    def resolve_data_path(self, data=None, filename=None):
        """Resolve data path for use with the executor.

        :param data: Data object instance
        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given data file in programs executed using this executor
        """
        if data is None:
            return constants.DATA_ALL_VOLUME

        # Prefix MUST be set because ``get_path`` uses Django's settings,
        # if prefix is not set, to get path prefix. But the executor
        # shouldn't use Django's settings directly, so prefix is set
        # via a constant.
        return data.location.get_path(
            prefix=constants.DATA_ALL_VOLUME, filename=filename
        )

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor
        """
        if filename is None:
            return constants.UPLOAD_VOLUME

        return constants.UPLOAD_VOLUME / filename

    def get_environment_variables(self):
        """Return dict of environment variables that will be added to executor."""

        return {"TMPDIR": os.fspath(constants.DATA_LOCAL_VOLUME / constants.TMPDIR)}
