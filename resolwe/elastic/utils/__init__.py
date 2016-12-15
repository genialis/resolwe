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
    """Convenience function to be used in `mapping` dict."""
    return lambda obj: con
