from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.apps import apps

from resolwe.flow.finders import BaseProcessesFinder


class AppDirectoriesProcessesFinder(BaseProcessesFinder):
    def _find_folders(self, folder_name):
        found_folders = []
        for app_config in apps.get_app_configs():
            folder_path = os.path.join(app_config.path, folder_name)
            if os.path.isdir(folder_path):
                found_folders.append(folder_path)
        return found_folders

    def find_processes(self):
        return self._find_folders('processes')

    def find_descriptors(self):
        return self._find_folders('descriptors')
