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
import os
import sys
import traceback
from contextlib import suppress
from importlib import import_module

import zmq
import zmq.asyncio

from .connectors import connectors
from .global_settings import initialize_constants
from .logger import configure_logging
from .socket_utils import Response, ResponseStatus
from .zeromq_utils import ZMQCommunicator

logger = logging.getLogger(__name__)

# Secrets necessary to connect to the listener service.
LISTENER_PUBLIC_KEY = os.getenv("LISTENER_PUBLIC_KEY").encode()
PUBLIC_KEY = os.getenv("CURVE_PUBLIC_KEY").encode()
PRIVATE_KEY = os.getenv("CURVE_PRIVATE_KEY").encode()


def handle_exception(exc_type, exc_value, exc_traceback):
    """Log unhandled exceptions."""
    message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.error("Unhandled exception in executor: {}".format(message))


sys.excepthook = handle_exception


def validate_response(response: Response):
    """Check the response status and raise excection on error.

    :raise RuntimeError: when response has error status.
    """
    if response.response_status == ResponseStatus.ERROR:
        raise RuntimeError(
            f"Response with error status received: '{response.message_data}'."
        )


async def open_listener_connection(data_id, host, port, protocol) -> ZMQCommunicator:
    """Connect to the listener service."""
    zmq_context = zmq.asyncio.Context.instance()
    zmq_socket = zmq_context.socket(zmq.DEALER)
    zmq_socket.curve_secretkey = PRIVATE_KEY
    zmq_socket.curve_publickey = PUBLIC_KEY
    zmq_socket.curve_serverkey = LISTENER_PUBLIC_KEY
    zmq_socket.setsockopt(zmq.IDENTITY, f"-{data_id}".encode())
    connect_string = f"{protocol}://{host}:{port}"
    zmq_socket.connect(connect_string)
    null_logger = logging.getLogger("Docker executor<->Listener")
    null_logger.propagate = False
    null_logger.handlers = []
    return ZMQCommunicator(zmq_socket, "executor<->listener", null_logger)


async def _run_executor():
    """Start the actual execution; instantiate the executor and run."""
    parser = argparse.ArgumentParser(description="Run the specified executor.")
    parser.add_argument(
        "module", help="The module from which to instantiate the concrete executor."
    )
    parser.add_argument(
        "data_id",
        type=int,
        help="The ID of the data object this executor will process.",
    )
    parser.add_argument("host", help="The address of the listener to connect to.")
    parser.add_argument(
        "port", type=int, help="The port of the listener to connect to."
    )
    parser.add_argument("protocol", help="The protocol of the listener to connect to.")

    args = parser.parse_args()

    communicator = await open_listener_connection(
        args.data_id, args.host, args.port, args.protocol
    )
    # Start listening for responses.
    asyncio.ensure_future(communicator.start_listening())
    configure_logging(communicator)

    response = await communicator.bootstrap((args.data_id, "executor"))
    validate_response(response)
    initialize_constants(args.data_id, response.message_data)
    connectors.recreate_connectors()

    module_name = "{}.run".format(args.module)
    class_name = "FlowExecutor"

    try:
        module = import_module(module_name, __package__)
        executor = getattr(module, class_name)(
            args.data_id, communicator, (args.host, args.port, args.protocol)
        )
        await executor.run()
    except:
        logger.exception("Unexpected exception while running executor %s.", module_name)


async def _close_tasks(pending_tasks, timeout=5):
    """Close still pending tasks."""
    if pending_tasks:
        # Give tasks time to cancel.
        with suppress(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.gather(*pending_tasks), timeout=timeout)
            await asyncio.gather(*pending_tasks)


async def start():
    """Start and stop all the services."""
    try:
        await _run_executor()
    finally:
        # TODO: remove asyncio.Task.all_tasks when we stop supporting Python 3.6.
        all_tasks = getattr(asyncio, "all_tasks", None) or asyncio.Task.all_tasks
        await _close_tasks(all_tasks())


if __name__ == "__main__":
    # TODO: remove this condition when we stop supporting Python 3.6.
    if sys.version_info < (3, 7):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start())
        loop.close()
    else:
        asyncio.run(start())
