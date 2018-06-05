""".. Ignore pydocstyle D400.

============
File finders
============

"""
import os

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


class BaseProcessesFinder:
    """Abstract processes loader for custom staticfiles finders."""

    def find_processes(self):
        """Abstract method."""
        raise NotImplementedError('subclasses of BaseProcessesLoader must provide a find_processes() method')

    def find_descriptors(self):
        """Abstract method."""
        raise NotImplementedError('subclasses of BaseProcessesLoader must provide a find_descriptors() method')


class FileSystemProcessesFinder(BaseProcessesFinder):
    """Find processes and descriptors in directory lists."""

    def find_processes(self):
        """Return a list of process directories."""
        return getattr(settings, 'FLOW_PROCESSES_DIRS', ())

    def find_descriptors(self):
        """Return a list of descriptor directories."""
        return getattr(settings, 'FLOW_DESCRIPTORS_DIRS', ())


class AppDirectoriesFinder(BaseProcessesFinder):
    """Find processes and descriptors in Django apps."""

    def _find_folders(self, folder_name):
        """Return a list of sub-directories."""
        found_folders = []
        for app_config in apps.get_app_configs():
            folder_path = os.path.join(app_config.path, folder_name)
            if os.path.isdir(folder_path):
                found_folders.append(folder_path)
        return found_folders

    def find_processes(self):
        """Return a list of process directories in the app."""
        return self._find_folders('processes')

    def find_descriptors(self):
        """Return a list of descriptor directories in the app."""
        return self._find_folders('descriptors')


def get_finders():
    """Get process finders."""
    for finder_path in settings.FLOW_PROCESSES_FINDERS:
        yield get_finder(finder_path)


def get_finder(import_path):
    """Get a process finder."""
    finder_class = import_string(import_path)
    if not issubclass(finder_class, BaseProcessesFinder):
        raise ImproperlyConfigured(
            'Finder "{}" is not a subclass of "{}"'.format(finder_class, BaseProcessesFinder))
    return finder_class()
