""".. Ignore pydocstyle D400.

==================
Test template tags
==================

"""
from django import template

register = template.Library()  # pylint: disable=invalid-name


@register.filter
def increase(value):
    """Test template tag that returns an increased value."""
    return value + 1
