""".. Ignore pydocstyle D400.

==============
Flow Utilities
==============

.. automodule:: resolwe.flow.utils.purge
   :members:

.. automodule:: resolwe.flow.utils.exceptions
   :members:

.. automodule:: resolwe.flow.utils.stats
   :members:

"""
import functools
import hashlib
import json
import os

from django.apps import apps
from django.conf import settings
from django.db import models

from .iterators import iterate_dict, iterate_fields, iterate_schema


def get_data_checksum(proc_input, proc_slug, proc_version):
    """Compute checksum of processor inputs, name and version."""
    checksum = hashlib.sha256()
    checksum.update(json.dumps(proc_input, sort_keys=True).encode('utf-8'))
    checksum.update(proc_slug.encode('utf-8'))
    checksum.update(str(proc_version).encode('utf-8'))
    return checksum.hexdigest()


def dict_dot(d, k, val=None, default=None):
    """Get or set value using a dot-notation key in a multilevel dict."""
    if val is None and k == '':
        return d

    def set_default(dict_or_model, key, default_value):
        """Set default field value."""
        if isinstance(dict_or_model, models.Model):
            if not hasattr(dict_or_model, key):
                setattr(dict_or_model, key, default_value)

            return getattr(dict_or_model, key)
        else:
            return dict_or_model.setdefault(key, default_value)

    def get_item(dict_or_model, key):
        """Get field value."""
        if isinstance(dict_or_model, models.Model):
            return getattr(dict_or_model, key)
        else:
            return dict_or_model[key]

    def set_item(dict_or_model, key, value):
        """Set field value."""
        if isinstance(dict_or_model, models.Model):
            setattr(dict_or_model, key, value)
        else:
            dict_or_model[key] = value

    if val is None and callable(default):
        # Get value, default for missing
        return functools.reduce(lambda a, b: set_default(a, b, default()), k.split('.'), d)

    elif val is None:
        # Get value, error on missing
        return functools.reduce(get_item, k.split('.'), d)

    else:
        # Set value
        try:
            k, k_last = k.rsplit('.', 1)
            set_item(dict_dot(d, k, default=dict), k_last, val)
        except ValueError:
            set_item(d, k, val)
        return val


def get_apps_tools():
    """Get applications' tools and their paths.

    Return a dict with application names as keys and paths to tools'
    directories as values. Applications without tools are omitted.
    """
    tools_paths = {}

    for app_config in apps.get_app_configs():
        proc_path = os.path.join(app_config.path, 'tools')
        if os.path.isdir(proc_path):
            tools_paths[app_config.name] = proc_path

    custom_tools_paths = getattr(settings, 'RESOLWE_CUSTOM_TOOLS_PATHS', [])
    if not isinstance(custom_tools_paths, list):
        raise KeyError("`RESOLWE_CUSTOM_TOOLS_PATHS` setting must be a list.")

    for seq, custom_path in enumerate(custom_tools_paths):
        custom_key = '_custom_{}'.format(seq)
        tools_paths[custom_key] = custom_path

    return tools_paths


def rewire_inputs(data_list):
    """Rewire inputs of provided data objects.

    Input parameter is a list of original and copied data object model
    instances: ``[{'original': original, 'copy': copy}]``. This
    function finds which objects reference other objects (in the list)
    on the input and replaces original objects with the copies (mutates
    copies' inputs).

    """
    if len(data_list) < 2:
        return data_list

    mapped_ids = {bundle['original'].id: bundle['copy'].id for bundle in data_list}

    for bundle in data_list:
        updated = False
        copy = bundle['copy']

        for field_schema, fields in iterate_fields(copy.input, copy.process.input_schema):
            name = field_schema['name']
            value = fields[name]

            if field_schema['type'].startswith('data:') and value in mapped_ids:
                fields[name] = mapped_ids[value]
                updated = True

            elif field_schema['type'].startswith('list:data:') and any([id_ in mapped_ids for id_ in value]):
                fields[name] = [mapped_ids[id_] if id_ in mapped_ids else id_ for id_ in value]
                updated = True

        if updated:
            copy.save()

    return data_list
