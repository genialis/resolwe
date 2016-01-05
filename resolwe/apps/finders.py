from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.apps import apps

from resolwe.flow.finders import BaseProcessesFinder


class AppDirectoriesProcessesFinder(BaseProcessesFinder):
    def find(self):
        proc_paths = []
        for app_config in apps.get_app_configs():
            proc_path = os.path.join(app_config.path, 'processes')
            if os.path.isdir(proc_path):
                proc_paths.append(proc_path)
        return proc_paths
