""".. Ignore pydocstyle D400.

==============
Flow Utilities
==============

.. automodule:: resolwe.flow.utils.purge
   :members:
.. automodule:: resolwe.flow.utils.exceptions
   :members:
.. automodule:: resolwe.flow.utils.test
   :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import hashlib


def get_data_checksum(proc_input, proc_slug, proc_version):
    """Compute checksum of processor inputs, name and version."""
    checksum = hashlib.sha1()
    checksum.update(str(proc_input).encode('utf-8'))
    checksum.update(proc_slug.encode('utf-8'))
    checksum.update(str(proc_version).encode('utf-8'))
    return checksum.hexdigest()
