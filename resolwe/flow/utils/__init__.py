""".. Ignore pydocstyle D400.

==============
Flow Utilities
==============

.. automodule:: resolwe.flow.utils.purge
   :members:

.. automodule:: resolwe.flow.utils.exceptions
   :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import functools
import hashlib
import json

from django.db import models

from .iterators import iterate_fields, iterate_schema  # pylint: disable=unused-import


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
