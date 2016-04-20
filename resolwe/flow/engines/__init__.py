"""Workflow compute engine"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil

from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils._os import upath


__all__ = ['manager']


def load_engine(engine_name):
    try:
        return import_module('{}'.format(engine_name))
    except ImportError as ex:
        # The executor wasn't found. Display a helpful error message
        # listing all possible (built-in) executors.
        executor_dir = os.path.join(os.path.dirname(upath(__file__)), 'engines')

        try:
            builtin_executors = [name for _, name, _ in pkgutil.iter_modules([executor_dir])]
        except EnvironmentError:
            builtin_executors = []
        if engine_name not in ['resolwe.flow.engines.{}'.format(b) for b in builtin_executors]:
            executor_reprs = map(repr, sorted(builtin_executors))
            error_msg = ("{} isn't an available engines.\n"
                         "Try using 'resolwe.flow.engines.XXX', where XXX is one of:\n"
                         "    {}\n"
                         "Error was: {}".format(engine_name, ", ".join(executor_reprs), ex))
            raise ImproperlyConfigured(error_msg)
        else:
            # If there's some other error, this must be an error in Django
            raise


settings_flow_engine = getattr(settings, 'FLOW_ENGINE', 'resolwe.flow.engines.local')
manager = load_engine(settings_flow_engine).Manager()  # pylint: disable=invalid-name
