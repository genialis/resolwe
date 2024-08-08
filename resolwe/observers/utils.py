"""Various utils in the observers app."""

import asyncio
from contextlib import suppress
from threading import Event, Thread
from typing import Awaitable, Optional

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
    request_user,
) -> BackgroundTask:
    """Create the BackgroundTask and start it."""
    task = BackgroundTask.objects.create(description=task_description)
    assign_contributor_permissions(task, request_user)
    packet = {
        "type": task_type.value,
        "task_id": task.id,
        "request_user_id": request_user.id,
        **task_data,
    }
    async_to_sync(get_channel_layer().send)(consumers.BACKGROUND_TASK_CHANNEL, packet)
    return task


class CoroutineInThread(Thread):
    """Start the coroutine in a new thread."""

    def __init__(self, coroutine: Awaitable):
        """Store a coroutine for later use."""
        super().__init__()
        self._coroutine = coroutine
        self._asyncio_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def wrapper(self):
        """Start the given coroutine and waits for its completion."""
        # The event must be initalized in the loop.
        self._loop = asyncio.get_running_loop()
        self._asyncio_task = asyncio.create_task(self._coroutine)
        with suppress(asyncio.CancelledError):
            await self._asyncio_task

    def run(self):
        """Start the new event loop."""
        asyncio.run(self.wrapper())

    def stop(self):
        """Stop the thread.

        The method blocks until the thread is stopped.
        """
        if self._asyncio_task is not None and self._loop is not None:
            self._loop.call_soon_threadsafe(self._asyncio_task.cancel)
            self.join()


class BackgroundTaskConsumerManager:
    """Start the background task consumer.

    The class can be used as a sync or async context manager. The task can be started
    inside given event loop or in a new thread.
    """

    def __init__(self):
        """Initialize the task attribute."""
        self._thread: Optional[CoroutineInThread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._task: Optional[asyncio.Task] = None
        self._coroutine_stopped: Event = Event()

    def set_loop(self, loop: Optional[asyncio.AbstractEventLoop]):
        """Set the event loop to run manager in.

        When loop is set to None, the manager is started in a new loop in a separate thread.
        """
        self._loop = loop

    def start_task(self):
        """Start the consumer task.

        When loop is set the task is started in the loop. Otherwise, the task is started
        in a new thread.

        If task is already started, the method has no effect.
        """
        if self._loop is not None:
            self._task = self._loop.create_task(self._listen_for_messages())

        elif self._thread is None:
            self._thread = CoroutineInThread(self._listen_for_messages())
            self._thread.start()

    def stop_task(self):
        """Stop the consumer task.

        When task is not started, the method has no effect.
        """
        if self._loop is not None and self._task is not None:
            self._loop.call_soon_threadsafe(self._task.cancel)
            self._coroutine_stopped.wait()
            self._coroutine_stopped.clear()
        elif self._thread is not None:
            self._thread.stop()
            self._thread = None

    async def _listen_for_messages(self):
        """Listen to messages and forward them to the consumer."""
        try:
            scope = {"channel": consumers.BACKGROUND_TASK_CHANNEL}
            app = ApplicationCommunicator(consumers.BackgroundTaskConsumer(), scope)
            channel_layer = get_channel_layer()
            while True:
                message = await channel_layer.receive(consumers.BACKGROUND_TASK_CHANNEL)
                await app.send_input({**message, **scope})
        finally:
            app.stop()
            self._coroutine_stopped.set()

    def __enter__(self):
        """Start the consumer task."""
        self.start_task()

    async def __aenter__(self):
        """Start the consumer task."""
        self.__enter__()

    def __exit__(self, *args, **kwargs):
        """Stop the consumer task."""
        self.stop_task()

    async def __aexit__(self, *args, **kwargs):
        """Stop the consumer task."""
        # Stop the task if it is running in the current loop.
        if self._loop == asyncio.get_running_loop() and self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        # Stop the task running in loop in a separate thread.
        else:
            self.__exit__(*args, **kwargs)


background_task_manager = BackgroundTaskConsumerManager()
