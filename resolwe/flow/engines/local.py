from __future__ import absolute_import, division, print_function, unicode_literals

from . import BaseManager


__all__ = ['manager']


class Manager(BaseManager):

    def run(self, data_id, script):
        self.executor.run(data_id, script)


manager = Manager()  # pylint: disable=invalid-name
