"""Logging configuration for executors."""
import asyncio
import json
import logging
import os
import socket

from .global_settings import DATA_ID
from .socket_utils import Message


class JSONFormatter(logging.Formatter):
    """Logger formatter for dumping records to JSON."""

    def format(self, record):
        """Dump the record to JSON."""
        data = record.__dict__.copy()

        data["data_id"] = DATA_ID
        data["hostname"] = socket.gethostname()

        # Get relative path, so listener can reconstruct the path to the actual code.
        data["pathname"] = os.path.relpath(data["pathname"], os.path.dirname(__file__))

        # Exception and Traceback cannot be serialized.
        data["exc_info"] = None

        return json.dumps(data, default=str)


class ZeromqHandler(logging.Handler):
    """Publish messages to Redis channel."""

    def __init__(self, communicator, **kwargs):
        """Construct a handler instance."""
        self.communicator = communicator
        self.loop = asyncio.get_event_loop()
        super().__init__(**kwargs)

    def emit(self, record):
        """Send log message to the listener."""
        asyncio.run_coroutine_threadsafe(
            self.communicator.send_command(Message.command("log", self.format(record))),
            self.loop,
        )


def configure_logging(communicator):
    """Configure logging to send log records to the master."""
    # Get and configure root logger.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Create and configure zmq handler.
    zmq_handler = ZeromqHandler(communicator)
    zmq_handler.setLevel(logging.DEBUG)
    zmq_handler.setFormatter(JSONFormatter())

    # Create and configure console_handler.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)

    # Add zeromq and console handlers to root logger.
    root_logger.addHandler(zmq_handler)
    root_logger.addHandler(console_handler)
