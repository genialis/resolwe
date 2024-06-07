""".. Ignore pydocstyle D400.

============
Celery Tasks
============

"""

import os
import subprocess
import sys

# Sphinx directly imports the modules it's documenting, so we need to
# guard from importing celery on installations which are configured to
# not use celery and thus don't have it available.
if "sphinx" not in sys.modules:
    from celery import shared_task
else:

    def shared_task(task):  # noqa: D103, pylint: disable=missing-docstring
        return task


@shared_task
def celery_run(
    data_id,
    runtime_dir,
    argv,
    listener_public_key: bytes,
    curve_public_key: bytes,
    curve_private_key: bytes,
):
    """Run process executor.

    :param data_id: The id of the :class:`~resolwe.flow.models.Data`
        object to be processed.
    :param runtime_dir: The directory from which to run the executor.
    :param argv: The argument vector used to run the executor.
    :param verbosity: The logging verbosity level.
    """
    process_environment = os.environ.copy()
    process_environment["LISTENER_PUBLIC_KEY"] = listener_public_key.decode()
    process_environment["CURVE_PUBLIC_KEY"] = curve_public_key.decode()
    process_environment["CURVE_PRIVATE_KEY"] = curve_private_key.decode()

    subprocess.Popen(
        argv, env=process_environment, cwd=runtime_dir, stdin=subprocess.DEVNULL
    ).wait()
