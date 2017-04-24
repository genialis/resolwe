""".. Ignore pydocstyle D400.

=============
Elastic Utils
=============

Collection of convenient functions and shortcuts that simplifies using
the app.

.. autofunction:: resolwe.elastic.utils.const

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.test import SimpleTestCase, override_settings

__all__ = ('const',)


def const(con):
    """Define a constant mapping for elastic search index.

    This helper may be used to define index mappings, where the indexed
    value is always set to a specific constant. Example:

    .. code-block:: python

        mapping = {'field': const('I am a constant')}

    """
    return lambda obj: con
