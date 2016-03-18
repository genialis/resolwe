from __future__ import absolute_import, division, print_function, unicode_literals


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


def get_finders():
    for finder_path in settings.FLOW_PROCESSES_FINDERS:
        yield get_finder(finder_path)


def get_finder(import_path):
    Finder = import_string(import_path)
    if not issubclass(Finder, BaseProcessesFinder):
        raise ImproperlyConfigured(
            'Finder "{}" is not a subclass of "{}"'.format(Finder, BaseProcessesFinder))
    return Finder()
