""".. Ignore pydocstyle D400.

============
Celery Tasks
============

"""
from __future__ import absolute_import

from celery import shared_task  # pylint: disable=import-error


@shared_task
def celery_run(data_id, script, verbosity):
    """Run process executor."""
    from .managers import manager
    manager.get_executor().run(data_id, script, verbosity)
