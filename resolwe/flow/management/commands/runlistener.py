""".. Ignore pydocstyle D400.

=================================
Standalone Executor Contact Point
=================================

Command to run on local machine::

    ./manage.py runlistener

"""

from django.conf import settings
from django.core.management.base import BaseCommand

from resolwe.flow.managers.listener import ExecutorListener


class Command(BaseCommand):
    """Run the executor listener."""

    help = "Run the standalone manager contact point for executors."

    def add_arguments(self, parser):
        """Add command arguments."""
        super(Command, self).add_arguments(parser)
        parser.add_argument('--clear-queue', action='store_true',
                            help="Consume and ignore any outstanding messages in the result queue on startup.")

    def handle(self, *args, **kwargs):
        """Run the executor listener. This method never returns."""
        listener = ExecutorListener(redis_params=getattr(settings, 'FLOW_MANAGER', {}).get('REDIS_CONNECTION', {}))
        if kwargs['clear_queue']:
            listener.clear_queue()
        listener.start()
        # listener.terminate() left out on purpose, this method never returns
        listener.join()
