"""Logging configuration for executors."""
import json
import logging
import os
import socket
from logging.config import dictConfig

from .global_settings import DATA
from .manager_commands import send_manager_command
from .protocol import ExecutorProtocol  # pylint: disable=import-error


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

        return json.dumps(data)


class RedisHandler(logging.Handler):
    """Publish messages to Redis channel."""

    def emit(self, record):
        """Send log message to the listener."""
        send_manager_command(
            ExecutorProtocol.LOG,
            extra_fields={
                ExecutorProtocol.LOG_MESSAGE: self.format(record),
            },
            expect_reply=False
        )


def configure_logging():
    """Configure logging to send log records to the master."""
    logging_config = dict(  # pylint: disable=invalid-name
        version=1,
        formatters={
            'json_formatter': {
                '()': JSONFormatter
            },
        },
        handlers={
            'redis': {
                'class': 'executors.logger.RedisHandler',
                'formatter': 'json_formatter',
                'level': logging.DEBUG
            },
            'console': {
                'class': 'logging.StreamHandler',
            },

        },
        root={
            'handlers': ['redis', 'console'],
            'level': logging.DEBUG,
        },
        loggers={
            # Don't use redis logger to prevent circular dependency.
            'executors.manager_comm': {
                'level': 'INFO',
                'handlers': ['console'],
                'propagate': False,
            },
        },
    )

    dictConfig(logging_config)
