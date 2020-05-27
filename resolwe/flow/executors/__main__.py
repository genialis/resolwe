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
from importlib import import_module

from . import manager_commands
from .global_settings import DATA, DATA_META
from .logger import configure_logging
from .protocol import ExecutorProtocol


def handle_exception(exc_type, exc_value, exc_traceback):
    """Log unhandled exceptions."""
    message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.error("Unhandled exception in executor: {}".format(message))

    loop.run_until_complete(
        asyncio.gather(
            *logging_future_list,
            manager_commands.send_manager_command(
                ExecutorProtocol.UPDATE,
                extra_fields={
                    ExecutorProtocol.UPDATE_CHANGESET: {
                        "process_error": ["Unhandled exception in executor."],
                        "status": DATA_META["STATUS_ERROR"],
                    }
                },
            ),
            manager_commands.send_manager_command(
                ExecutorProtocol.ABORT, expect_reply=False
            ),
        )
    )


sys.excepthook = handle_exception


async def run_executor():
    """Start the actual execution; instantiate the executor and run."""
    parser = argparse.ArgumentParser(description="Run the specified executor.")
    parser.add_argument(
        "module", help="The module from which to instantiate the concrete executor."
    )
    args = parser.parse_args()

    module_name = "{}.run".format(args.module)
    class_name = "FlowExecutor"

    module = import_module(module_name, __package__)
    executor = getattr(module, class_name)()
    with open(ExecutorFiles.PROCESS_SCRIPT, "rt") as script_file:
        await executor.run(DATA["id"], script_file.read())


if __name__ == "__main__":
    # Initialize logging ASAP.
    logging_future_list = []
    loop = asyncio.get_event_loop()
    configure_logging(logging_future_list)
    loop.run_until_complete(manager_commands.init())
    logger = logging.getLogger(__name__)

    # Import storage modules.
    from .collect import collect_files
    from .protocol import ExecutorFiles
    from .transfer import transfer_data

    async def _sequential():
        """Run some things sequentially but asynchronously."""
        await transfer_data()
        await run_executor()
        await collect_files()

    loop.run_until_complete(_sequential())

    # Wait for any pending logging emits now there's
    # nothing else running anymore
    loop.run_until_complete(asyncio.gather(*logging_future_list))

    # Now that logging is done too, close the connection cleanly.
    loop.run_until_complete(manager_commands.deinit())

    # Any stragglers?

    # NOTE: method bellow is deprecated in Python 3.7 in favor of
    # asyncio.all_tasks and will be removed in Python 3.9. The method
    # asycio.all_tasks has been added in Python 3.7 so a switch can be made
    # when support for Python 3.6 is dropped in Resolwe.
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))

    loop.close()
