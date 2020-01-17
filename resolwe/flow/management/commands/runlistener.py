""".. Ignore pydocstyle D400.

=================================
Standalone Executor Contact Point
=================================

Command to run on local machine::

    ./manage.py runlistener

"""

import asyncio
from signal import SIGINT, SIGTERM, signal

from django.conf import settings
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
        listener = ExecutorListener(
            redis_params=getattr(settings, "FLOW_MANAGER", {}).get(
                "REDIS_CONNECTION", {}
            )
        )

        def _killer(signum, frame):
            """Kill the listener on receipt of a signal."""
            listener.terminate()

        signal(SIGINT, _killer)
        signal(SIGTERM, _killer)

        async def _runner():
            """Run the listener instance."""
            if kwargs["clear_queue"]:
                await listener.clear_queue()
            async with listener:
                pass

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_runner())
        loop.close()
