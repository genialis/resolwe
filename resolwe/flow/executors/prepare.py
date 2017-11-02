"""Base class for the manager-internal executor preparation bits."""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os

from django.apps import apps
from django.conf import settings
from django.forms.models import model_to_dict

from resolwe.flow.managers.protocol import ExecutorFiles
from resolwe.flow.models import Data

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class BaseFlowExecutorPreparer(object):
    """Represents the preparation functionality of the executor."""

    def extend_settings(self, data_id, files):
        """Extend the settings the manager will serialize.

        :param data_id: The :class:`~resolwe.flow.models.Data` object id
            being prepared for.
        :param files: The settings dictionary to be serialized. Keys are
            filenames, values are the objects that will be serialized
            into those files. Standard filenames are listed in
            :class:`resolwe.flow.managers.protocol.ExecutorFiles`.
        """
        files[ExecutorFiles.DJANGO_SETTINGS].update({
            'USE_TZ': settings.USE_TZ,
            'FLOW_EXECUTOR_TOOLS_PATHS': self.get_tools(),
        })
        files[ExecutorFiles.DATA] = model_to_dict(Data.objects.get(
            pk=data_id
        ))
        files[ExecutorFiles.PROCESS] = model_to_dict(Data.objects.get(
            pk=data_id
        ).process)

    def get_tools(self):
        """Get tools paths."""
        tools_paths = []
        for app_config in apps.get_app_configs():
            proc_path = os.path.join(app_config.path, 'tools')
            if os.path.isdir(proc_path):
                tools_paths.append(proc_path)

        custom_tools_paths = getattr(settings, 'RESOLWE_CUSTOM_TOOLS_PATHS', [])
        if not isinstance(custom_tools_paths, list):
            raise KeyError("`RESOLWE_CUSTOM_TOOLS_PATHS` setting must be a list.")
        tools_paths.extend(custom_tools_paths)

        return tools_paths

    def post_register_hook(self):
        """Run hook after the 'register' management command finishes.

        Subclasses may implement this hook to e.g. pull Docker images at
        this point. By default, it does nothing.
        """
        pass
