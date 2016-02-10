"""
===========================
Process Fields Templatetags
===========================

"""
from django import template


register = template.Library()  # pylint: disable=invalid-name


@register.filter
def id(obj):  # pylint: disable=redefined-builtin,invalid-name
    """Return ``id`` key of dict."""
    return obj['__id']


@register.filter
def type(obj):  # pylint: disable=redefined-builtin
    """Return ``type`` key of dict."""
    return obj['__type']
