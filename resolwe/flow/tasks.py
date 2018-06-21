""".. Ignore pydocstyle D400.

============
Celery Tasks
============

"""
import subprocess
import sys

# Sphinx directly imports the modules it's documenting, so we need to
# guard from importing celery on installations which are configured to
# not use celery and thus don't have it available.
if 'sphinx' not in sys.modules:
    from celery import shared_task  # pylint: disable=import-error
else:
    def shared_task(task):  # noqa: D103, pylint: disable=missing-docstring
        return task


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
