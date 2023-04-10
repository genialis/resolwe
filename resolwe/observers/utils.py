"""Various utils in the observers app."""

import asyncio
from contextlib import suppress
from typing import Optional

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from resolwe.observers import consumers


def start_background_task(packet: dict):
    """Send the packet to background task consumer."""
    async_to_sync(get_channel_layer().send)(consumers.BACKGROUND_TASK_CHANNEL, packet)


class BackgroundTaskConsumerManager:
    """The background task consumer context manager.

    Starts the background task consumer in its context.

    The manager is reentrant and reusable.
    """

    def __init__(self) -> None:
        """Initialize constants."""
        self._background_task: Optional[asyncio.Task] = None

    async def _consumer_task(self):
        """Start the consumer."""
        try:
            scope = {"channel": consumers.BACKGROUND_TASK_CHANNEL}
            app = ApplicationCommunicator(consumers.BackgroundTaskConsumer(), scope)
            channel_layer = get_channel_layer()
            while True:
                message = await channel_layer.receive(consumers.BACKGROUND_TASK_CHANNEL)
                message.update(scope)
                await app.send_input(message)
        finally:
            app.stop()

    def stop_background_task(self):
        """Stop the background job."""
        if self._background_task is not None:
            self._background_task.cancel()

    async def __aenter__(self):
        """Start the consumer task and return it."""
        if self._background_task is None:
            self._background_task = asyncio.create_task(self._consumer_task())
        return self._background_task

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the consumer task and wait for it to finish."""
        if self._background_task is not None:
            self.stop_background_task()
            with suppress(Exception):
                await self._background_task()
            self._background_task = None


background_task_manager = BackgroundTaskConsumerManager()
