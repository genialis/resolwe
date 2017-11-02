"""Workflow workload managers."""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil
from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils._os import upath

__all__ = ('manager')


def load_manager(manager_name):
    """Load manager."""
    try:
        return import_module('{}'.format(manager_name))
    except ImportError as ex:
        # The manager wasn't found. Display a helpful error message
        # listing all possible (built-in) managers.
        manager_dir = os.path.join(os.path.dirname(upath(__file__)), 'managers')

        try:
            builtin_managers = [name for _, name, _ in pkgutil.iter_modules([manager_dir])]
        except EnvironmentError:
            builtin_managers = []
        if manager_name not in ['resolwe.flow.managers.{}'.format(b) for b in builtin_managers]:
            manager_reprs = map(repr, sorted(builtin_managers))
            error_msg = ("{} isn't an available manager.\n"
                         "Try using 'resolwe.flow.managers.XXX', where XXX is one of:\n"
                         "    {}\n"
                         "Error was: {}".format(manager_name, ", ".join(manager_reprs), ex))
            raise ImproperlyConfigured(error_msg)
        else:
            # If there's some other error, this must be an error in Django
            raise


FLOW_MANAGER = getattr(settings, 'FLOW_MANAGER', {}).get('NAME', 'resolwe.flow.managers.local')
manager = load_manager(FLOW_MANAGER).Manager(static=True)  # pylint: disable=invalid-name
