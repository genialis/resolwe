""".. Ignore pydocstyle D400.

============
Celery Tasks
============

"""
from __future__ import absolute_import

import subprocess

from celery import shared_task  # pylint: disable=import-error


@shared_task
def celery_run(data_id, runtime_dir, argv):
    """Run process executor.

    :param data_id: The id of the :class:`~resolwe.flow.models.Data`
        object to be processed.
    :param runtime_dir: The directory from which to run the executor.
    :param argv: The argument vector used to run the executor.
    :param verbosity: The logging verbosity level.
    """
    subprocess.Popen(
        argv,
        cwd=runtime_dir,
        stdin=subprocess.DEVNULL
    ).wait()
