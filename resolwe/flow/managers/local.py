"""
=============
Local Manager
=============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from .base import BaseManager


class Manager(BaseManager):

    """Local manager for job execution."""

    def run(self, data_id, script, run_sync=False, verbosity=1):
        """Run process locally."""
        self.executor.run(data_id, script, verbosity=verbosity)
