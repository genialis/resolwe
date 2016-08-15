"""
============================
Process fields template tags
============================

"""
from django import template

from resolwe.flow.models import Data


register = template.Library()  # pylint: disable=invalid-name


def _get_data_attr(data, attr):
    """Get data object field."""
    if isinstance(data, dict):
        # `Data` object's id is hydrated as `__id` in expression engine
        data = data['__id']

    data_obj = Data.objects.get(id=data)

    return getattr(data_obj, attr)


@register.filter
def name(data):
    """Return `name` of `Data`."""
    return _get_data_attr(data, 'name')


@register.filter
def id(obj):  # pylint: disable=redefined-builtin,invalid-name
    """Return ``id`` key of dict."""
    return obj['__id']


@register.filter
def type(obj):  # pylint: disable=redefined-builtin
    """Return ``type`` key of dict."""
    return obj['__type']
