"""
================
Resource Filters
================

"""
import os
from django import template

register = template.Library()  # pylint: disable=invalid-name


@register.filter
def basename(path):
    """Return the base name of pathname path."""
    return os.path.basename(path)


@register.filter
def subtype(type_, supertype):
    """Check if type_ is a subtype of supertype."""
    return type_.startswith(supertype)
