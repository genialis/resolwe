""".. Ignore pydocstyle D400.

======================
Flow Executor Preparer
======================

Framework for the manager-resident executor preparation facilities.

.. autoclass:: resolwe.flow.executors.prepare.BaseFlowExecutorPreparer
    :members:

"""

import logging
import os

from django.conf import settings

from resolwe.flow.utils import get_apps_tools
from resolwe.storage import settings as storage_settings
from resolwe.storage.connectors import connectors
from resolwe.test.utils import is_testing

logger = logging.getLogger(__name__)


class BaseFlowExecutorPreparer:
    """Represents the preparation functionality of the executor."""

    def prepare_for_execution(self, data):
        """Prepare the data object for the execution.

        This is mostly needed for the null executor to change the status of
        the data and worker object to done.
        """

    def extend_settings(self, data_id, files, secrets):
        """Extend the settings the manager will serialize.

        :param data_id: The :class:`~resolwe.flow.models.Data` object id
            being prepared for.
        :param files: The settings dictionary to be serialized. Keys are
            filenames, values are the objects that will be serialized
            into those files. Standard filenames are listed in
            ``resolwe.flow.managers.protocol.ExecutorFiles``.
        :param secrets: Secret files dictionary describing additional secret
            file content that should be created and made available to
            processes with special permissions. Keys are filenames, values
            are the raw strings that should be written into those files.
        """
        pass

    def get_tools_paths(self, from_applications=False):
        """Get tools' paths."""
        if settings.DEBUG or is_testing() or from_applications:
            return list(get_apps_tools().values())

        else:
            tools_root = storage_settings.FLOW_VOLUMES["tools"]["config"]["path"]
        try:
            subdirs = next(os.walk(tools_root))[1]
            return [os.path.join(tools_root, sdir) for sdir in subdirs]
        except StopIteration:
            return []

    def post_register_hook(self, verbosity=1):
        """Run hook after the 'register' management command finishes.

        Subclasses may implement this hook to e.g. pull Docker images at
        this point. By default, it does nothing.
        """

    def resolve_data_path(self, data=None, filename=None):
        """Resolve data path for use with the executor.

        :param data: Data object instance
        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given data file in programs executed using this executor
        """
        if data is None:
            filesystem_connectors = [
                connector
                for connector in connectors.for_storage("data")
                if connector.mountable
            ]
            return filesystem_connectors[0].path if filesystem_connectors else None

        return data.location.get_path(filename=filename)

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor

        :raises RuntimeError: when no storage connectors are configured for
          upload storage or path could not be resolved.
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
            return upload_connector.path
        else:
            return os.path.join(upload_connector.path, filename)

    def get_environment_variables(self):
        """Return dict of environment variables that will be added to executor."""

        return {}
