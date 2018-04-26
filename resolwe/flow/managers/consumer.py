"""Manager Channels consumer."""
from asgiref.sync import async_to_sync
from channels.consumer import SyncConsumer
from channels.layers import get_channel_layer

from . import state


def send_manager_event(message):
    """Construct a channels event packet with the given message.

    :param message: The message to send to the manager workers.
    """
    packet = {
        'type': 'control_event',  # This is used as the method name in the consumer.
        'content': message
    }
    async_to_sync(get_channel_layer().send)(state.MANAGER_CONTROL_CHANNEL, packet)


class ManagerConsumer(SyncConsumer):
    """Channels consumer for handling manager events."""

    def __init__(self, *args, **kwargs):
        """Initialize a consumer instance with the given manager."""
        # This import is local in order to avoid startup import loops.
        from . import manager
        self.manager = manager
        super().__init__(*args, **kwargs)

    def control_event(self, message):
        """Forward control events to the manager dispatcher."""
        self.manager.handle_control_event(message['content'])
