""".. Ignore pydocstyle D400.

==============
Celery Manager
==============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from ..tasks import celery_run
from .base import BaseManager

try:
    import celery  # pylint: disable=unused-import
except ImportError:
    print('Please install Celery using `pip install celery`', file=sys.stderr)
    sys.exit(1)


class Manager(BaseManager):
    """Celey-based manager for job execution."""

    def run(self, data_id, script, priority='normal', run_sync=False, verbosity=1):
        """Run process."""
        queue = 'ordinary'
        if priority == 'high':
            queue = 'hipri'

        celery_run.apply_async((data_id, script, verbosity), queue=queue)
