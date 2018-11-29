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
from resolwe.test.utils import is_testing

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class BaseFlowExecutorPreparer:
    """Represents the preparation functionality of the executor."""

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
        data = Data.objects.select_related('process').get(pk=data_id)

        files[ExecutorFiles.DJANGO_SETTINGS].update({
            'USE_TZ': settings.USE_TZ,
            'FLOW_EXECUTOR_TOOLS_PATHS': self.get_tools_paths(),
        })
        files[ExecutorFiles.DATA] = model_to_dict(data)
        files[ExecutorFiles.PROCESS] = model_to_dict(data.process)
        files[ExecutorFiles.PROCESS]['resource_limits'] = data.process.get_resource_limits()

        # Add secrets if the process has permission to read them.
        secrets.update(data.resolve_secrets())

    def get_tools_paths(self):
        """Get tools' paths."""
        if settings.DEBUG or is_testing():
            return list(get_apps_tools().values())

        else:
            tools_root = settings.FLOW_TOOLS_ROOT
            subdirs = next(os.walk(tools_root))[1]

            return [os.path.join(tools_root, sdir) for sdir in subdirs]

    def post_register_hook(self, verbosity=1):
        """Run hook after the 'register' management command finishes.

        Subclasses may implement this hook to e.g. pull Docker images at
        this point. By default, it does nothing.
        """
        pass

    def resolve_data_path(self, data=None, filename=None):
        """Resolve data path for use with the executor.

        :param data: Data object instance
        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given data file in programs executed using this executor
        """
        if data is None:
            return settings.FLOW_EXECUTOR['DATA_DIR']

        if filename is None:
            return os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))

        return os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id), filename)

    def resolve_upload_path(self, filename=None):
        """Resolve upload path for use with the executor.

        :param filename: Filename to resolve
        :return: Resolved filename, which can be used to access the
            given uploaded file in programs executed using this
            executor
        """
        if filename is None:
            return settings.FLOW_EXECUTOR['UPLOAD_DIR']

        return os.path.join(settings.FLOW_EXECUTOR['UPLOAD_DIR'], filename)

    def get_environment_variables(self):
        """Return dict of environment variables that will be added to executor."""
        return {}
