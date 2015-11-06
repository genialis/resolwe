from __future__ import absolute_import, division, print_function, unicode_literals

import sys

try:
    import celery
except ImportError:
    print('Please install Celery using `pip install celery`', file=sys.stderr)
    sys.exit(1)


from . import BaseManager
from ..tasks import celery_run


__all__ = ['manager']


class Manager(BaseManager):

    def run(self, data_id, script):
        celery_run.async(self.executor, data_id, script)


manager = Manager()  # pylint: disable=invalid-name
