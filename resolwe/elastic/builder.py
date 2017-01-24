""".. Ignore pydocstyle D400.

=====================
Elastic Index Builder
=====================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from importlib import import_module
import inspect
import re

from elasticsearch_dsl.connections import connections

from django.apps import apps
from django.conf import settings
from django.db.models.signals import post_save, pre_delete

from .indices import BaseIndex


__all__ = ('index_builder',)


class ElasticSignal(object):
    """Class for creating signals to update/create indexes.

    To register the signal, add the following code::

        from django.dispatch import receiver

        signal = ElasticSignal(<my_signal>, <method_name>)
        receiver(<signal_type>, [sender=<my_model>])(signal)

    ``signal type`` can be i.e. ``django.db.models.signals.pre_save``.

    """

    def __init__(self, index, method_name):
        """Initialize signal."""
        self.index = index
        self.method_name = method_name

    def __call__(self, sender, instance, **kwargs):
        """Process signal."""
        method = getattr(self.index, self.method_name)
        method(obj=instance)


class IndexBuilder(object):
    """Find indexes and register coresponding signals.

    Indexes are auto collected from ``elastic_indexes.py`` files from
    all Django registered apps

    Post-save and pre-delete signals are registered for objects
    specified in ``object_type`` attribute of each index.

    """

    #: list of index builders
    indexes = []

    #: list of registered signals
    signals = []

    def __init__(self):
        """Initialize index builder object."""
        # Set dafault connection for ElasticSearch
        elasticsearch_host = getattr(settings, 'ELASTICSEARCH_HOST', 'localhost')  # pylint: disable=invalid-name
        elasticsearch_port = getattr(settings, 'ELASTICSEARCH_PORT', 9200)  # pylint: disable=invalid-name
        connections.create_connection(hosts=['{}:{}'.format(elasticsearch_host, elasticsearch_port)])

        self.discover_indexes()
        self.create_mappings()
        self.register_signals()

    def _connect_signal(self, index):
        """Create signals for building indexes."""
        post_save_signal = ElasticSignal(index, 'build')
        self.signals.append(post_save_signal)
        post_save.connect(post_save_signal, sender=index.object_type)

        pre_delete_signal = ElasticSignal(index, 'remove_object')
        self.signals.append(pre_delete_signal)
        pre_delete.connect(pre_delete_signal, sender=index.object_type)

    def unregister_signals(self):
        """Delete signals for building indexes."""
        for signal in self.signals:
            post_save.disconnect(signal, sender=signal.index.object_type)
            pre_delete.disconnect(signal, sender=signal.index.object_type)
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

    def destroy(self):
        """Delete all entries from ElasticSearch."""
        for index in self.indexes:
            index.destroy()
            index.create_mapping()

    def remove_object(self, obj):
        """Delete given object from all indexes."""
        for index in self.indexes:
            index.remove_object(obj)


index_builder = IndexBuilder()  # pylint: disable=invalid-name
