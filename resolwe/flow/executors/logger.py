"""Logging configuration for executors."""
import asyncio
import json
import logging
import os
import socket
import sys
from logging.config import dictConfig

from .global_settings import DATA
from .manager_commands import send_manager_command
from .protocol import ExecutorProtocol


class JSONFormatter(logging.Formatter):
    """Logger formatter for dumping records to JSON."""

    def format(self, record):
        """Dump the record to JSON."""
        data = record.__dict__.copy()

        data['data_id'] = DATA['id']
        data['hostname'] = socket.gethostname()

        # Get relative path, so listener can reconstruct the path to the actual code.
        data['pathname'] = os.path.relpath(data['pathname'], os.path.dirname(__file__))

        # Exception and Traceback cannot be serialized.
        data['exc_info'] = None

        # Ensure logging message is instantiated to a string.
        data['msg'] = str(data['msg'])

        return json.dumps(data)


class RedisHandler(logging.Handler):
    """Publish messages to Redis channel."""

    def __init__(self, emit_list, **kwargs):
        """Construct a handler instance.

        :param emit_list: The list to add emit futures into, so they can
            be waited on in the executor main function.
        """
        self.emit_list = emit_list
        super().__init__(**kwargs)

    def emit(self, record):
        """Send log message to the listener."""
        future = asyncio.ensure_future(send_manager_command(
            ExecutorProtocol.LOG,
            extra_fields={
                ExecutorProtocol.LOG_MESSAGE: self.format(record),
            },
            expect_reply=False
        ))
        self.emit_list.append(future)


def configure_logging(emit_list):
    """Configure logging to send log records to the master."""
    if 'sphinx' in sys.modules:
        module_base = 'resolwe.flow.executors'
    else:
        module_base = 'executors'
    logging_config = dict(  # pylint: disable=invalid-name
        version=1,
        formatters={
            'json_formatter': {
                '()': JSONFormatter
            },
        },
        handlers={
            'redis': {
                'class': module_base + '.logger.RedisHandler',
                'formatter': 'json_formatter',
                'level': logging.INFO,
                'emit_list': emit_list
            },
            'console': {
                'class': 'logging.StreamHandler',
                'level': logging.WARNING
            },

        },
        root={
            'handlers': ['redis', 'console'],
            'level': logging.DEBUG,
        },
        loggers={
            # Don't use redis logger to prevent circular dependency.
            module_base + '.manager_comm': {
                'level': 'INFO',
                'handlers': ['console'],
                'propagate': False,
            },
        },
    )

    dictConfig(logging_config)
