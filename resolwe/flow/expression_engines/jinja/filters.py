"""Filters for Jinja expression engine."""
from __future__ import absolute_import, division, print_function, unicode_literals

import os

from resolwe.flow.models import Data


def _get_data_attr(data, attr):
    """Get data object field."""
    if isinstance(data, dict):
        # `Data` object's id is hydrated as `__id` in expression engine
        data = data['__id']

    data_obj = Data.objects.get(id=data)

    return getattr(data_obj, attr)


def name(data):
    """Return `name` of `Data`."""
    return _get_data_attr(data, 'name')


def id_(obj):
    """Return ``id`` key of dict."""
    return obj['__id']


def type_(obj):
    """Return ``type`` key of dict."""
    return obj['__type']


def basename(path):
    """Return the base name of pathname path."""
    return os.path.basename(path)


def subtype(basetype, supertype):
    """Check if ``basetype`` is a subtype of supertype."""
    return basetype.startswith(supertype)


def yesno(value, true_value, false_value):
    """Return ``true_value`` if ``value`` evaluates to true and ``false_value`` otherwise."""
    return true_value if value else false_value


def data_by_slug(slug):
    """Return the primary key of a data object identified by the given slug."""
    return Data.objects.get(slug=slug).pk


# A dictionary of filters that will be registered.
filters = {  # pylint: disable=invalid-name
    'name': name,
    'id': id_,
    'type': type_,
    'basename': basename,
    'subtype': subtype,
    'yesno': yesno,
    'data_by_slug': data_by_slug,
}
