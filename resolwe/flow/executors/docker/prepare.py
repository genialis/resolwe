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

from resolwe.storage.connectors import connectors

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
        :raises RuntimeError: when data path can not be resolved.
        """
        storage_name = "data"
        filesystem_connectors = [
            connector
            for connector in connectors.for_storage(storage_name)
            if connector.mountable
        ]

        if data is None:
            if not filesystem_connectors:
                return constants.INPUTS_VOLUME
            else:
                return f"/{storage_name}_{filesystem_connectors[0].name}"

        data_connectors = data.location.connectors
        for connector in filesystem_connectors:
            if connector in data_connectors:
                return data.location.get_path(
                    prefix=f"/{storage_name}_{connector.name}", filename=filename
                )

        return data.location.get_path(prefix=constants.INPUTS_VOLUME, filename=filename)

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor
        """
        upload_connectors = [
            connector
            for connector in connectors.for_storage("upload")
            if connector.mountable
        ]
        if not upload_connectors:
            raise RuntimeError("No connectors are configured for 'upload' storage.")

        upload_connector = upload_connectors[0]
        if filename is None:
            return f"/upload_{upload_connector.name}"

        return f"/upload_{upload_connector.name}/{filename}"

    def get_environment_variables(self):
        """Return dict of environment variables that will be added to executor."""

        return {"TMPDIR": os.fspath(constants.PROCESSING_VOLUME / constants.TMPDIR)}
