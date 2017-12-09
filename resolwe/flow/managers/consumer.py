"""Manager Channels consumer."""
from channels.generic import BaseConsumer

from . import manager, state


class ManagerConsumer(BaseConsumer):
    """Channels consumer for handling manager events."""

    @classmethod
    def channel_names(cls):
        """Names of channels to listen for messages on."""
        return {state.MANAGER_CONTROL_CHANNEL}

    @property
    def method_mapping(self):
        """Consumer channel to method mapping."""
        return {
            state.MANAGER_CONTROL_CHANNEL: 'handle_control_event',
        }

    def handle_control_event(self, message):
        """Handle a control event message."""
        manager.handle_control_event(message)
