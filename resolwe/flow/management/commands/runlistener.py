""".. Ignore pydocstyle D400.

=================================
Standalone Executor Contact Point
=================================

Command to run on local machine::

    ./manage.py runlistener

"""

import asyncio
from signal import SIGINT, SIGTERM

import uvloop
from django.core.management.base import BaseCommand

from resolwe.flow.managers.listener import ExecutorListener


class Command(BaseCommand):
    """Run the executor listener."""

    help = "Run the standalone manager contact point for executors."

    def add_arguments(self, parser):
        """Add command arguments."""
        super().add_arguments(parser)
        parser.add_argument(
            "--clear-queue",
            action="store_true",
            help="Consume and ignore any outstanding messages in the result queue on startup.",
        )

    def handle(self, *args, **kwargs):
        """Run the executor listener. This method never returns."""

        async def _runner():
            """Run the listener instance."""

            listener = ExecutorListener()

            async def _killer():
                """Kill the listener on receipt of a signal."""
                listener.terminate()

            loop.add_signal_handler(SIGINT, lambda: asyncio.ensure_future(_killer()))
            loop.add_signal_handler(SIGTERM, lambda: asyncio.ensure_future(_killer()))
            async with listener:
                await listener.should_stop.wait()

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = asyncio.new_event_loop()

        loop.run_until_complete(_runner())
        loop.close()
