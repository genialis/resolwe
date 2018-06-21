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

from . import constants
from ..prepare import BaseFlowExecutorPreparer


class FlowExecutorPreparer(BaseFlowExecutorPreparer):
    """Specialized manager assist for the docker executor."""

    def post_register_hook(self, verbosity=1):
        """Pull Docker images needed by processes after registering."""
        if not getattr(settings, 'FLOW_DOCKER_DONT_PULL', False):
            call_command('list_docker_images', pull=True, verbosity=verbosity)

    def resolve_data_path(self, data=None, filename=None):
        """Resolve data path for use with the executor.

        :param data: Data object instance
        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given data file in programs executed using this executor
        """
        if data is None:
            return constants.DATA_ALL_VOLUME

        if filename is None:
            return os.path.join(constants.DATA_ALL_VOLUME, str(data.id))

        return os.path.join(constants.DATA_ALL_VOLUME, str(data.id), filename)

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor
        """
        if filename is None:
            return constants.UPLOAD_VOLUME

        return os.path.join(constants.UPLOAD_VOLUME, filename)
