""".. Ignore pydocstyle D400.

=============
Local Manager
=============

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import subprocess

from resolwe.utils import BraceMessage as __

from .base import BaseManager

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Manager(BaseManager):
    """Local manager for job execution."""

    def run(self, data_id, dest_dir, argv, priority='normal', run_sync=False, verbosity=1):
        """Run process locally.

        For details, see
        :meth:`~resolwe.flow.managers.base.BaseManager.run`.
        """
        logger.debug(__(
            "Manager '{}.{}' running {}.",
            self.__class__.__module__,
            self.__class__.__name__,
            repr(argv)
        ))
        subprocess.Popen(
            argv,
            cwd=dest_dir,
            stdin=subprocess.DEVNULL
        ).wait()
