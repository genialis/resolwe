""".. Ignore pydocstyle D400.

==============
Celery Manager
==============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

try:
    import celery  # pylint: disable=unused-import
except ImportError:
    print('Please install Celery using `pip install celery`', file=sys.stderr)
    sys.exit(1)

from .base import BaseManager
from ..tasks import celery_run


class Manager(BaseManager):
    """Celey-based manager for job execution."""

    def run(self, data_id, script, run_sync=False, verbosity=1):
        """Run process."""
        celery_run.delay(data_id, script, verbosity)
