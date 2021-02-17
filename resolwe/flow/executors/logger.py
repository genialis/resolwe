"""Logging configuration for executors."""
import asyncio
import json
import logging
import os
import socket
import sys
from logging.config import dictConfig

import zmq
import zmq.asyncio

from .global_settings import DATA, SETTINGS, STORAGE_LOCATION
from .socket_utils import Message
from .zeromq_utils import ZMQCommunicator


class JSONFormatter(logging.Formatter):
    """Logger formatter for dumping records to JSON."""

    def format(self, record):
        """Dump the record to JSON."""
        data = record.__dict__.copy()

        data["data_id"] = DATA["id"]
        data["data_location_id"] = STORAGE_LOCATION["id"]
        data["hostname"] = socket.gethostname()

        # Get relative path, so listener can reconstruct the path to the actual code.
        data["pathname"] = os.path.relpath(data["pathname"], os.path.dirname(__file__))

        # Exception and Traceback cannot be serialized.
        data["exc_info"] = None

        return json.dumps(data, default=str)


class ZeromqHandler(logging.Handler):
    """Publish messages to Redis channel."""

    def __init__(self, **kwargs):
        """Construct a handler instance.

        :param emit_list: The list to add emit futures into, so they can
            be waited on in the executor main function.
        """
        self.communicator = self.open_listener_connection(DATA["id"])
        self.loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.communicator.start_listening())
        super().__init__(**kwargs)

    def open_listener_connection(self, data_id: int) -> ZMQCommunicator:
        """Connect to the listener service.

        We are using data id as identity. This implies only one process per
        data object at any given point in time can be running.
        """
        zmq_context = zmq.asyncio.Context.instance()
        zmq_socket = zmq_context.socket(zmq.DEALER)
        zmq_socket.setsockopt(zmq.IDENTITY, f"e{data_id}".encode())

        listener_settings = SETTINGS.get("FLOW_EXECUTOR", {}).get(
            "LISTENER_CONNECTION", {}
        )

        LISTENER_IP = listener_settings.get("hosts", {}).get("docker", "127.0.0.1")
        LISTENER_PROTOCOL = listener_settings.get("protocol", "tcp")
        LISTENER_PORT = listener_settings.get("port", 53893)

        connect_string = f"{LISTENER_PROTOCOL}://{LISTENER_IP}:{LISTENER_PORT}"
        zmq_socket.connect(connect_string)

        null_logger = logging.getLogger("Docker executor<->Listener")
        null_logger.handlers = []

        return ZMQCommunicator(zmq_socket, "docker logger", null_logger)

    def emit(self, record):
        """Send log message to the listener."""
        asyncio.run_coroutine_threadsafe(
            self.communicator.send_command(Message.command("log", self.format(record))),
            self.loop,
        )


def configure_logging():
    """Configure logging to send log records to the master."""
    if "sphinx" in sys.modules:
        module_base = "resolwe.flow.executors"
    else:
        module_base = "executors"
    logging_config = dict(
        version=1,
        formatters={
            "json_formatter": {"()": JSONFormatter},
        },
        handlers={
            "zeromq": {
                "class": module_base + ".logger.ZeromqHandler",
                "formatter": "json_formatter",
                "level": logging.DEBUG,
            },
            "console": {"class": "logging.StreamHandler", "level": logging.WARNING},
        },
        root={
            "handlers": ["zeromq", "console"],
            "level": logging.DEBUG,
        },
        loggers={
            # Don't use zeromq logger to prevent circular dependency.
            module_base
            + ".manager_comm": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    )
    dictConfig(logging_config)
