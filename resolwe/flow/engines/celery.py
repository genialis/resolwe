from __future__ import absolute_import, division, print_function, unicode_literals

import sys

try:
    import celery
except ImportError:
    print('Please install Celery using `pip install celery`', file=sys.stderr)
    sys.exit(1)


from .base import BaseManager
from ..tasks import celery_run


class Manager(BaseManager):

    def run(self, data_id, script, run_sync=False, verbosity=1):
        celery_run.delay(self.executor, data_id, script, verbosity)
