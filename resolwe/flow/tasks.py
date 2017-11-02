""".. Ignore pydocstyle D400.

============
Celery Tasks
============

"""
from __future__ import absolute_import

import subprocess

from celery import shared_task  # pylint: disable=import-error


@shared_task
def celery_run(data_id, dest_dir, argv, verbosity):
    """Run process executor.

    :param data_id: The id of the :class:`~resolwe.flow.models.Data`
        object to be processed.
    :param dest_dir: The directory from which to run the executor.
    :param argv: The argument vector used to run the executor.
    :param verbosity: The logging verbosity level.
    """
    subprocess.Popen(
        argv,
        cwd=dest_dir,
        stdin=subprocess.DEVNULL
    ).wait()
