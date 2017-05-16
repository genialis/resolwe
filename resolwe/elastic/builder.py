""".. Ignore pydocstyle D400.

=====================
Elastic Index Builder
=====================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
import re
from importlib import import_module

from django.apps import apps
from django.db import models
from django.db.models.fields.related_descriptors import ManyToManyDescriptor
from django.db.models.signals import m2m_changed, post_save, pre_delete

from .indices import BaseIndex
from .utils import prepare_connection

__all__ = (
    'index_builder',
    'ManyToManyDependency',
)


class ElasticSignal(object):
    """Class for creating signals to update/create indexes.

    To register the signal, add the following code::

        signal = ElasticSignal(<my_signal>, <method_name>)
        signal.connect(<signal_type>, [sender=<my_model>])

    You may later disconnect the signal by calling::

        signal.disconnect()

    ``signal type`` can be i.e. ``django.db.models.signals.pre_save``.

    """

    def __init__(self, index, method_name, pass_kwargs=False):
        """Initialize signal."""
        self.index = index
        self.method_name = method_name
        self.pass_kwargs = pass_kwargs
        self.connections = []

    def connect(self, signal, **kwargs):
        """Connect a specific signal type to this receiver."""
        signal.connect(self, **kwargs)
        self.connections.append((signal, kwargs))

    def disconnect(self):
        """Disconnect all connected signal types from this receiver."""
        for signal, kwargs in self.connections:
            signal.disconnect(self, **kwargs)

    def __call__(self, sender, instance, **kwargs):
        """Process signal."""
        method = getattr(self.index, self.method_name)
        if self.pass_kwargs:
            method(obj=instance, **kwargs)
        else:
            method(obj=instance)


class Dependency(object):
    """Abstract base class for index model dependencies."""

    def __init__(self, model):
        """Construct dependency."""
        self.model = model
        self.index = None

    def connect(self, index):
        """Connect signals needed for dependency updates."""
        self.index = index

        signal = ElasticSignal(self, 'process', pass_kwargs=True)
        signal.connect(post_save, sender=self.model)
        signal.connect(pre_delete, sender=self.model)
        return signal

    def process(self, obj, **kwargs):
        """Process signals from dependencies."""
        raise NotImplementedError


class ManyToManyDependency(Dependency):
    """Dependency on a many-to-many relation."""

    def __init__(self, field):
        """Construct m2m dependency."""
        super(ManyToManyDependency, self).__init__(field.rel.to)
        self.field = field

    def connect(self, index):
        """Connect signals needed for dependency updates."""
        signal = super(ManyToManyDependency, self).connect(index)
        signal.connect(m2m_changed, sender=self.field.through)
        return signal

    def filter(self, obj, update_fields=None):
        """Determine if dependent object should be processed.

        If ``False`` is returned, processing of the dependent object will
        be aborted.
        """
        pass

    def process(self, obj, pk_set=None, action=None, update_fields=None, **kwargs):
        """Process signals from dependencies."""
        if action not in (None, 'post_add', 'post_remove', 'post_clear'):
            return

        if isinstance(obj, self.index.object_type):
            if action != 'post_clear':
                # Check filter before rebuilding index.
                filtered = [
                    dep
                    for dep in self.field.rel.to.objects.filter(pk__in=pk_set)
                    if self.filter(dep) is not False
                ]

                if not filtered:
                    return

            self.index.build(obj)
        elif isinstance(obj, self.field.rel.to):
            # Check filter before rebuilding index.
            if self.filter(obj, update_fields=update_fields) is False:
                return

            for instance in getattr(obj, self.field.rel.get_accessor_name()).all():
                self.index.build(instance, push=False)
            self.index.push()


class IndexBuilder(object):
    """Find indexes and register corresponding signals.

    Indexes are auto collected from ``elastic_indexes.py`` files from
    all Django registered apps

    Post-save and pre-delete signals are registered for objects
    specified in ``object_type`` attribute of each index.

    """

    def __init__(self):
        """Initialize index builder object."""
        #: list of index builders
        self.indexes = []

        #: list of registered signals
        self.signals = []

        prepare_connection()

        self.discover_indexes()
        self.create_mappings()
        self.register_signals()

    def _connect_signal(self, index):
        """Create signals for building indexes."""
        post_save_signal = ElasticSignal(index, 'build')
        post_save_signal.connect(post_save, sender=index.object_type)
        self.signals.append(post_save_signal)

        pre_delete_signal = ElasticSignal(index, 'remove_object')
        pre_delete_signal.connect(pre_delete, sender=index.object_type)
        self.signals.append(pre_delete_signal)

        # Connect signals for all dependencies.
        for dependency in index.get_dependencies():
            # Automatically convert m2m fields to dependencies.
            if isinstance(dependency, (models.ManyToManyField, ManyToManyDescriptor)):
                dependency = ManyToManyDependency(dependency)
            elif not isinstance(dependency, Dependency):
                raise TypeError("Unsupported dependency type: {}".format(repr(dependency)))

            signal = dependency.connect(index)
            self.signals.append(signal)

    def unregister_signals(self):
        """Delete signals for building indexes."""
        for signal in self.signals:
            signal.disconnect()
        self.signals = []

    def register_signals(self):
        """Register signals for all indexes."""
        for index in self.indexes:
            if index.object_type:
                self._connect_signal(index)

    def create_mappings(self):
        """Create mappings for all indexes."""
        for index in self.indexes:
            index.create_mapping()

    def discover_indexes(self):
        """Save list of index builders into ``_index_builders``."""
        for app_config in apps.get_app_configs():
            indexes_path = '{}.elastic_indexes'.format(app_config.name)
            try:
                indexes_module = import_module(indexes_path)

                for attr_name in dir(indexes_module):
                    attr = getattr(indexes_module, attr_name)
                    if inspect.isclass(attr) and issubclass(attr, BaseIndex) and attr is not BaseIndex:
                        self.indexes.append(attr())
            except ImportError as ex:
                if not re.match('No module named .*elastic_indexes.*', str(ex)):
                    raise

    def build(self, obj=None, push=True):
        """Trigger building of the indexes.

        Support passing ``obj`` parameter to the indexes, so we can
        trigger build only for one object.
        """
        for index in self.indexes:
            index.build(obj, push)

    def push(self, index=None):
        """Push built documents to ElasticSearch.

        If ``index`` is specified, only that index will be pushed.
        """
        for ind in self.indexes:
            if index and not isinstance(ind, index):
                continue
            ind.push()

    def delete(self):
        """Delete all entries from ElasticSearch."""
        for index in self.indexes:
            index.destroy()
            index.create_mapping()

    def remove_object(self, obj):
        """Delete given object from all indexes."""
        for index in self.indexes:
            index.remove_object(obj)

    def destroy(self):
        """Delete all indexes from Elasticsearch and index builder."""
        self.unregister_signals()
        for index in self.indexes:
            index.destroy()
        self.indexes = []


index_builder = IndexBuilder()  # pylint: disable=invalid-name
