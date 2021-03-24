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
from django.forms.models import model_to_dict

from resolwe.flow.managers.protocol import ExecutorFiles
from resolwe.flow.models import Data
from resolwe.flow.utils import get_apps_tools
from resolwe.storage.connectors import connectors
from resolwe.storage import settings as storage_settings
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

    def get_tools_paths(self):
        """Get tools' paths."""
        if settings.DEBUG or is_testing():
            return list(get_apps_tools().values())

        else:
            tools_root = storage_settings.FLOW_VOLUMES["tools"]["path"]
            subdirs = next(os.walk(tools_root))[1]

            return [os.path.join(tools_root, sdir) for sdir in subdirs]

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
        data_dir = settings.FLOW_EXECUTOR["DATA_DIR"]
        if data is None:
            return data_dir

        return data.location.get_path(filename=filename)

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor
        """
        if filename is None:
            return settings.FLOW_EXECUTOR["UPLOAD_DIR"]

        return os.path.join(settings.FLOW_EXECUTOR["UPLOAD_DIR"], filename)

    def get_environment_variables(self):
        """Return dict of environment variables that will be added to executor."""

        return {}
