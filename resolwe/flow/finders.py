from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


class BaseProcessesFinder(object):
    """
    A base processes loader to be used for custom staticfiles finder
    classes.
    """

    def find_processes(self):
        raise NotImplementedError('subclasses of BaseProcessesLoader must provide a find_processes() method')

    def find_descriptors(self):
        raise NotImplementedError('subclasses of BaseProcessesLoader must provide a find_descriptors() method')


class FileSystemProcessesFinder(BaseProcessesFinder):
    def find_processes(self):
        return getattr(settings, 'FLOW_PROCESSES_DIRS', ())

    def find_descriptors(self):
        return getattr(settings, 'FLOW_DESCRIPTORS_DIRS', ())


class AppDirectoriesFinder(BaseProcessesFinder):
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


def get_finders():
    for finder_path in settings.FLOW_PROCESSES_FINDERS:
        yield get_finder(finder_path)


def get_finder(import_path):
    Finder = import_string(import_path)
    if not issubclass(Finder, BaseProcessesFinder):
        raise ImproperlyConfigured(
            'Finder "{}" is not a subclass of "{}"'.format(Finder, BaseProcessesFinder))
    return Finder()
