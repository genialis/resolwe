"""Various utils in the observers app."""

import asyncio
from contextlib import suppress

from channels.db import database_sync_to_async
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from resolwe.observers.consumers import BACKGROUND_TASK_CHANNEL, BackgroundTaskConsumer


def with_background_task_consumer(test_case):
    """Use to decorate the sync test case.

    When decorated, the test case will become async and background task consomer ill be
    running for the duration of the test.
    """

    async def wrapper(*args, **kwargs):
        async with BackgroundTaskConsumerManager():
            await database_sync_to_async(test_case, thread_sensitive=False)(
                *args, **kwargs
            )

    return wrapper


class BackgroundTaskConsumerManager:
    """Start the background task consumer.

    The context manager is helpfull while testing. Example:

    async with BackgroundTaskConsumerManager() as task:
        methods requiring background task consumer to run.
    """

    async def _consumer_task(self):
        """Start the consumer."""
        try:
            scope = {"channel": BACKGROUND_TASK_CHANNEL}
            app = ApplicationCommunicator(BackgroundTaskConsumer(), scope)
            channel_layer = get_channel_layer()
            while True:
                message = await channel_layer.receive(BACKGROUND_TASK_CHANNEL)
                message.update(scope)
                await app.send_input(message)
        finally:
            app.stop()

    async def __aenter__(self):
        """Start the consumer task and return it."""
        self._consumer_task = asyncio.create_task(self._consumer_task())
        return self._consumer_task

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the consumer task and wait for it to finish."""
        self._consumer_task.cancel()
        with suppress(Exception):
            await self._consumer_task()
