"""Consumers for Observers."""

from typing import Dict, List, Union

from channels.generic.websocket import JsonWebsocketConsumer

from django.contrib.contenttypes.models import ContentType

from .models import Observer, Subscription
from .protocol import GROUP_SESSIONS, ChangeType


class ClientConsumer(JsonWebsocketConsumer):
    """Consumer for client communication."""

    def websocket_connect(self, event: Dict[str, str]):
        """Handle establishing a WebSocket connection."""
        session_id: str = self.scope["url_route"]["kwargs"]["session_id"]
        self.session_id = session_id

        # Accept the connection.
        super().websocket_connect(event)

    @property
    def groups(self) -> List[str]:
        """Generate a list of groups this channel should add itself to."""
        if not hasattr(self, "session_id"):
            return []
        return [GROUP_SESSIONS.format(session_id=self.session_id)]

    def disconnect(self, code: int):
        """Handle closing the WebSocket connection."""
        Subscription.objects.filter(session_id=self.session_id).delete()
        self.close()

    def observers_item_update(self, msg: Dict[str, Union[str, int]]):
        """Handle an item update signal."""
        content_type = ContentType.objects.get_for_id(msg["content_type_pk"])
        object_id = msg["object_id"]
        change_type = ChangeType(msg["change_type_value"])

        interested = Observer.get_interested(
            content_type=content_type, object_id=object_id, change_type=change_type
        )
        subscription_ids = [
            subscription_id.hex
            for subscription_id in Subscription.objects.filter(observers__in=interested)
            .filter(session_id=self.session_id)
            .values_list("subscription_id", flat=True)
            .distinct()
        ]

        if change_type == ChangeType.DELETE:
            # The observed object was either deleted or the user lost permissions.
            subscription = Subscription.objects.get(session_id=self.session_id)
            observers = Observer.objects.filter(
                content_type=content_type,
                object_id=object_id,
            )
            # Assure we don't stay subscribed to an illegal object.
            subscription.observers.remove(*observers)

        for subscription_id in subscription_ids:
            self.send_json(
                {
                    "subscription_id": subscription_id,
                    "object_id": object_id,
                    "change_type": change_type.name,
                }
            )
