from __future__ import absolute_import

from celery import shared_task


@shared_task
def celery_run(executor, data_id, script, verbosity):
    executor.run(data_id, script, verbosity)
