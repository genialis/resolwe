""".. Ignore pydocstyle D400.

========
Consumer
========

Manager Channels consumer.

"""
import asyncio
import logging
import os
from contextlib import suppress
from pathlib import Path

from channels.consumer import AsyncConsumer
from channels.db import database_sync_to_async
from channels.layers import get_channel_layer
from channels.testing import ApplicationCommunicator

from django.db import connection

from resolwe.utils import BraceMessage as __

from . import state

logger = logging.getLogger(__name__)
CHANNEL_HEALTH_CHECK = os.environ.get("HOSTNAME")


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
    channel_layer = get_channel_layer()

    async def _consume_loop(channel, scope, app):
        """Run a loop to consume messages off the channels layer."""
        message = await channel_layer.receive(channel)
        while message.get("type", {}) != "_resolwe_manager_quit":
            message.update(scope)
            await app.send_input(message)
            message = await channel_layer.receive(channel)

    consume_future = asyncio.ensure_future(
        _consume_loop(manager_channel, manager_scope, manager_app)
    )

    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(consume_future, timeout=timeout)

    await manager_app.wait()


async def exit_consumer():
    """Cause the synchronous consumer to exit cleanly."""
    packet = {
        "type": "_resolwe_manager_quit",
    }
    await get_channel_layer().send(state.MANAGER_CONTROL_CHANNEL, packet)


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


class HealtCheckConsumer(AsyncConsumer):
    """Channels consumer for handling health-check events."""

    async def health_check(self, message: dict):
        """Perform health check.

        We are testing the channels layer and database layer. The channels
        layer is already functioning if this method is called so we have to
        perform database check.

        If the check is successfull touch the file specified in the channels
        message.
        """
        logger.debug(__("Performing health check with message {}.", message))
        path = Path(message["file"])

        if await self.check_database():
            logger.debug("Health check passed.")
            path.touch(exist_ok=True)

    @database_sync_to_async
    def check_database(self) -> bool:
        """Perform a simple database check."""
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()[0]
            return result == 1
