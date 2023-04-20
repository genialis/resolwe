"""Various utils in the observers app."""

import asyncio
from contextlib import suppress

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from resolwe.observers import consumers
from resolwe.observers.models import BackgroundTask
from resolwe.permissions.utils import assign_contributor_permissions


def start_background_task(
    task_type: consumers.BackgroundTaskType,
    task_description: str,
    task_data: dict,
    contributor,
) -> BackgroundTask:
    """Create the BackgroundTask and start it."""
    task = BackgroundTask.objects.create(description=task_description)
    assign_contributor_permissions(task, contributor)
    packet = {
        "type": task_type.value,
        "task_id": task.id,
        "contributor_id": contributor.id,
        **task_data,
    }
    async_to_sync(get_channel_layer().send)(consumers.BACKGROUND_TASK_CHANNEL, packet)
    return task


class BackgroundTaskConsumerManager:
    """Start the background task consumer.

    The context manager is helpfull while testing. Example:

    async with BackgroundTaskConsumerManager() as task:
        methods requiring background task consumer to run.
    """

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

    async def __aenter__(self):
        """Start the consumer task and return it."""
        self._consumer_task = asyncio.create_task(self._consumer_task())
        return self._consumer_task

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the consumer task and wait for it to finish."""
        self._consumer_task.cancel()
        with suppress(Exception):
            await self._consumer_task()
