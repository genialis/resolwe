"""Filters for Jinja expression engine."""
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os

from django.conf import settings

from resolwe.flow.models import Data
from resolwe.flow.utils import dict_dot


def apply_filter_list(func, obj):
    """Apply `func` to list or tuple `obj` element-wise and directly otherwise."""
    if isinstance(obj, (list, tuple)):
        return [func(item) for item in obj]
    return func(obj)


def _get_data_attr(data, attr):
    """Get data object field."""
    if isinstance(data, dict):
        # `Data` object's id is hydrated as `__id` in expression engine
        data = data['__id']

    data_obj = Data.objects.get(id=data)

    return getattr(data_obj, attr)


def name(data):
    """Return `name` of `Data`."""
    return apply_filter_list(lambda datum: _get_data_attr(datum, 'name'), data)


def id_(obj):
    """Return ``id`` key of dict."""
    return apply_filter_list(lambda item: item['__id'], obj)


def type_(obj):
    """Return ``type`` key of dict."""
    return apply_filter_list(lambda item: item['__type'], obj)


def basename(path):
    """Return the base name of pathname path."""
    return os.path.basename(path)


def subtype(basetype, supertype):
    """Check if ``basetype`` is a subtype of supertype."""
    return apply_filter_list(lambda item: item.startswith(supertype), basetype)


def yesno(value, true_value, false_value):
    """Return ``true_value`` if ``value`` evaluates to true and ``false_value`` otherwise."""
    return true_value if value else false_value


def data_by_slug(slug):
    """Return the primary key of a data object identified by the given slug."""
    return Data.objects.get(slug=slug).pk


def get_url(hydrated_path):
    """Return file's url based on base url set in settings."""
    # Get only file path if whole file object is given.
    if isinstance(hydrated_path, dict) and 'file' in hydrated_path:
        hydrated_path = hydrated_path['file']

    if not hasattr(hydrated_path, 'file_name'):
        raise TypeError("Argument to get_url must be a hydrated path")

    base_url = getattr(settings, 'RESOLWE_HOST_URL', 'localhost')
    return "{}/data/{}/{}".format(base_url, hydrated_path.data_id, hydrated_path.file_name)


def descriptor(obj, path=''):
    """Return descriptor of given object.

    If ``path`` is specified, only the content on that path is
    returned.
    """
    if isinstance(obj, dict):
        # Current object is hydrated, so we need to get descriptor from
        # dict representation.
        desc = obj['__descriptor']
    else:
        desc = obj.descriptor

    resp = dict_dot(desc, path)

    if isinstance(resp, list) or isinstance(resp, dict):
        return json.dumps(resp)

    return resp


def all_(obj):
    """Return True if all items in obj are true or if obj is empty."""
    return all(obj)


def any_(obj):
    """Return True if any item in obj is true. If obj is empty, return False."""
    return any(obj)


# A dictionary of filters that will be registered.
filters = {  # pylint: disable=invalid-name
    'name': name,
    'id': id_,
    'type': type_,
    'basename': basename,
    'subtype': subtype,
    'yesno': yesno,
    'data_by_slug': data_by_slug,
    'get_url': get_url,
    'descriptor': descriptor,
    'all': all_,
    'any': any_,
}
