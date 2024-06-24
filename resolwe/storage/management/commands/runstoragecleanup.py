""".. Ignore pydocstyle D400.

==========================
Command: runstoragecleanup
==========================

"""

import logging

from asgiref.sync import async_to_sync
from channels.layers import ChannelFull, get_channel_layer
from django.core.management.base import BaseCommand

from resolwe.storage.protocol import (
    CHANNEL_STORAGE_CLEANUP_WORKER,
    TYPE_STORAGE_CLEANUP_RUN,
)

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Start storage cleanup run via signal by django channels."""

    help = "Start cleanup manager run via signal by django channels."

    def handle(self, *args, **options):
        """Command handle."""
        channel_layer = get_channel_layer()
        try:
            async_to_sync(channel_layer.send)(
                CHANNEL_STORAGE_CLEANUP_WORKER,
                {
                    "type": TYPE_STORAGE_CLEANUP_RUN,
                },
            )
        except ChannelFull:
            logger.warning(
                "Cannot trigger storage manager run because channel is full.",
            )
