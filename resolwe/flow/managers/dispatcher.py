"""Front-end manager dispatcher class."""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from resolwe.flow.models import Process

from .base import BaseManager

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Manager(BaseManager):
    """The front-end manager dispatcher class.

    When a data object is run, the dispatcher will select an appropriate
    manager for it, instantiate that manager and hand over the data
    object for processing.
    """

    def __init__(self, *args, **kwargs):
        """Construct a dispatcher instance and check settings."""
        if 'DISPATCHER_MAPPING' not in getattr(settings, 'FLOW_MANAGER', {}):
            raise ImproperlyConfigured("Required key 'DISPATCHER_MAPPING' not in FLOW_MANAGER settings.")

        mapping = settings.FLOW_MANAGER['DISPATCHER_MAPPING']
        self.scheduling_class_map = dict(Process.SCHEDULING_CLASS_CHOICES)

        scheduling_classes = set(self.scheduling_class_map.values())
        map_keys = set(mapping.keys())
        if not scheduling_classes <= map_keys:
            raise ImproperlyConfigured("Dispatcher manager mapping settings incomplete.")

        super().__init__(*args, **kwargs)

    def run(self, data, dest_dir, argv, run_sync=False, verbosity=1):
        """Select a concrete manager and run the object through it.

        For details, see
        :meth:`~resolwe.flow.managers.base.BaseManager.run`.
        """
        process_scheduling = self.scheduling_class_map[data.process.scheduling_class]
        manager_class = settings.FLOW_MANAGER['DISPATCHER_MAPPING'][process_scheduling]
        manager_module = import_module(manager_class)
        manager = manager_module.Manager(internal=True)
        return manager.run(data, dest_dir, argv, run_sync, verbosity)
