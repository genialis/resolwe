"""Main standalone execution stub, used when the executor is run.

It should be run as a module with one argument: the relative module name
of the concrete executor class to use. The current working directory
should be where the ``executors`` module directory is, so that it can be
imported with python's ``-m <module>`` interpreter option.

Usage format:

.. code-block:: none

    /path/to/python -m executors .executor_type

Concrete example, run from the directory where ``./executors/`` is:

.. code-block:: none

    /venv/bin/python -m executors .docker

using the python from the ``venv`` virtualenv.

.. note::

    The startup code adds the concrete class name as needed, so that in
    the example above, what's actually instantiated is
    ``.docker.run.FlowExecutor``.
"""

import argparse
import asyncio
import logging
import sys
import traceback
from contextlib import suppress
from importlib import import_module

from .global_settings import DATA
from .logger import configure_logging

logger = logging.getLogger(__name__)


def handle_exception(exc_type, exc_value, exc_traceback):
    """Log unhandled exceptions."""
    message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.error("Unhandled exception in executor: {}".format(message))


sys.excepthook = handle_exception


async def _run_executor():
    """Start the actual execution; instantiate the executor and run."""
    parser = argparse.ArgumentParser(description="Run the specified executor.")
    parser.add_argument(
        "module", help="The module from which to instantiate the concrete executor."
    )
    args = parser.parse_args()

    module_name = "{}.run".format(args.module)
    class_name = "FlowExecutor"

    try:
        module = import_module(module_name, __package__)
        executor = getattr(module, class_name)(DATA["id"])
        await executor.run()
    except:
        logger.exception("Unhandled exception in executor, aborting.")


async def _close_tasks(pending_tasks, timeout=5):
    """Close still pending tasks."""
    if pending_tasks:
        # Give tasks time to cancel.
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.gather(*pending_tasks), timeout=timeout)
            await asyncio.gather(*pending_tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    configure_logging()
    loop.run_until_complete(_run_executor())
    loop.run_until_complete(_close_tasks(asyncio.Task.all_tasks()))
    loop.close()
