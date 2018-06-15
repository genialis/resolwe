"""Manager Channels consumer."""
import asyncio

import async_timeout
from channels.consumer import AsyncConsumer
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from . import state


async def send_event(message):
    """Construct a Channels event packet with the given message.

    :param message: The message to send to the manager workers.
    """
    packet = {
        'type': 'control_event',  # This is used as the method name in the consumer.
        'content': message,
    }
    await get_channel_layer().send(state.MANAGER_CONTROL_CHANNEL, packet)


async def run_consumer(timeout=None):
    """Run the consumer until it finishes processing."""
    channel = state.MANAGER_CONTROL_CHANNEL
    scope = {
        'type': 'control_event',
        'channel': channel,
    }
    from . import manager

    app = ApplicationCommunicator(ManagerConsumer, scope)

    channel_layer = get_channel_layer()

    async def _consume_loop():
        """Run a loop to consume messages off the channels layer."""
        while True:
            message = await channel_layer.receive(channel)
            if message.get('type', {}) == '_resolwe_manager_quit':
                break
            message.update(scope)
            await manager.sync_counter.inc('message-handling')
            await app.send_input(message)

    if timeout is None:
        await _consume_loop()
    try:
        # A further grace period to catch late messages.
        async with async_timeout.timeout(timeout or 1):
            await _consume_loop()
    except asyncio.TimeoutError:
        pass

    await app.wait()
    flush = getattr(channel_layer, 'flush', None)
    if flush:
        await flush()


async def exit_consumer():
    """Cause the synchronous consumer to exit cleanly."""
    packet = {
        'type': '_resolwe_manager_quit',
    }
    await get_channel_layer().send(state.MANAGER_CONTROL_CHANNEL, packet)


class ManagerConsumer(AsyncConsumer):
    """Channels consumer for handling manager events."""

    def __init__(self, *args, **kwargs):
        """Initialize a consumer instance with the given manager."""
        # This import is local in order to avoid startup import loops.
        from . import manager
        self.manager = manager
        super().__init__(*args, **kwargs)

    async def control_event(self, message):
        """Forward control events to the manager dispatcher."""
        try:
            await self.manager.handle_control_event(message['content'])
        finally:
            await self.manager.sync_counter.dec('message-handling')
