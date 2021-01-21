""".. Ignore pydocstyle D400.

========
Consumer
========

Manager Channels consumer.

"""
import asyncio
import logging

from channels.consumer import AsyncConsumer
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from . import state

logger = logging.getLogger(__name__)


async def send_event(message):
    """Construct a Channels event packet with the given message.

    :param message: The message to send to the manager workers.
    """
    packet = {
        "type": "control_event",  # This is used as the method name in the consumer.
        "content": message,
    }
    await get_channel_layer().send(state.MANAGER_CONTROL_CHANNEL, packet)


async def run_consumer(timeout=None):
    """Run the consumer until it finishes processing.

    :param timeout: Set maximum execution time before cancellation, or
        ``None`` (default) for unlimited.
    """
    manager_channel = state.MANAGER_CONTROL_CHANNEL
    manager_scope = {
        "type": "control_event",
        "channel": manager_channel,
    }
    manager_app = ApplicationCommunicator(ManagerConsumer(), manager_scope)

    listener_channel = state.LISTENER_CONTROL_CHANNEL
    listener_scope = {
        "channel": listener_channel,
    }
    listener_app = ApplicationCommunicator(ListenerConsumer(), listener_scope)

    channel_layer = get_channel_layer()

    async def _consume_loop(channel, scope, app):
        """Run a loop to consume messages off the channels layer."""
        message = await channel_layer.receive(channel)
        while message.get("type", {}) != "_resolwe_manager_quit":
            message.update(scope)
            await app.send_input(message)
            message = await channel_layer.receive(channel)

    awaitables = [
        asyncio.ensure_future(
            _consume_loop(manager_channel, manager_scope, manager_app)
        ),
        asyncio.ensure_future(
            _consume_loop(listener_channel, listener_scope, listener_app)
        ),
    ]

    await asyncio.wait(awaitables, timeout=timeout)
    for awaitable in awaitables:
        awaitable.cancel()

    await manager_app.wait()
    await listener_app.wait()


async def exit_consumer():
    """Cause the synchronous consumer to exit cleanly."""
    packet = {
        "type": "_resolwe_manager_quit",
    }
    await get_channel_layer().send(state.MANAGER_CONTROL_CHANNEL, packet)
    await get_channel_layer().send(state.LISTENER_CONTROL_CHANNEL, packet)


class ManagerConsumer(AsyncConsumer):
    """Channels consumer for handling manager events."""

    def __init__(self, *args, **kwargs):
        """Initialize a consumer instance with the given manager."""
        # This import is local in order to avoid startup import loops.
        from . import manager

        self.manager = manager

    async def control_event(self, message):
        """Forward control events to the manager dispatcher."""
        logger.debug("control_event got message %s", message)
        try:
            await self.manager.handle_control_event(message["content"])
        except:
            logger.exception("control_event exception.")


class ListenerConsumer(AsyncConsumer):
    """Channels consumer for handling manager events."""

    def __init__(self, *args, **kwargs):
        """Initialize a consumer instance with the given manager."""
        # This import is local in order to avoid startup import loops.
        from . import listener

        self.listener = listener

    async def terminate_worker_event(self, message):
        """Forward control events to the manager dispatcher."""
        await self.listener.terminate_worker(message["identity"])
