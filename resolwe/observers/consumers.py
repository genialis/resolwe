"""Consumers for Observers."""

from typing import Callable

from channels.consumer import SyncConsumer
from channels.generic.websocket import JsonWebsocketConsumer

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.utils import timezone

from resolwe.flow.models.utils import bulk_duplicate

from .models import BackgroundTask, Observer, Subscription
from .protocol import GROUP_SESSIONS, ChangeType, ChannelsMessage, WebsocketMessage

# The channel used to listen for BackgrountTask events
BACKGROUND_TASK_CHANNEL = "observers.background_task"
from django.contrib.auth import get_user_model


class ClientConsumer(JsonWebsocketConsumer):
    """Consumer for client communication."""

    def websocket_connect(self, event: dict[str, str]):
        """Handle establishing a WebSocket connection."""
        session_id: str = self.scope["url_route"]["kwargs"]["session_id"]
        self.session_id = session_id

        # Accept the connection.
        super().websocket_connect(event)

    @property
    def groups(self) -> list[str]:
        """Generate a list of groups this channel should add itself to."""
        if not hasattr(self, "session_id"):
            return []
        return [GROUP_SESSIONS.format(session_id=self.session_id)]

    def disconnect(self, code: int):
        """Handle closing the WebSocket connection."""
        Subscription.objects.filter(session_id=self.session_id).delete()

    def observers_item_update(self, msg: ChannelsMessage):
        """Handle an item update signal."""
        content_type = ContentType.objects.get_for_id(msg["content_type_pk"])
        object_id = msg["object_id"]
        change_type = ChangeType(msg["change_type_value"])
        source = msg["source"]

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

        is_object_source = source == (content_type.name, object_id)
        if change_type == ChangeType.DELETE and is_object_source:
            # The observed object was either deleted or the user lost permissions.
            subscription = Subscription.objects.get(session_id=self.session_id)
            observers = Observer.objects.filter(
                content_type=content_type,
                object_id=object_id,
            )
            # Assure we don't stay subscribed to an illegal object.
            subscription.observers.remove(*observers)

        to_send: WebsocketMessage = {
            "object_id": object_id,
            "change_type": change_type.name,
            "subscription_id": "0",
            "source": source,
        }

        for subscription_id in subscription_ids:
            to_send["subscription_id"] = subscription_id
            self.send_json(to_send)


class BackgroundTaskConsumer(SyncConsumer):
    """The background task consumer."""

    def wrap_task(self, function: Callable, task: BackgroundTask):
        """Start the function and update background task status."""
        try:
            task.started = timezone.now()
            task.status = BackgroundTask.STATUS_PROCESSING
            task.save(update_fields=["status", "started"])
            task.output = function()
            task.status = BackgroundTask.STATUS_DONE
        except ValidationError as e:
            task.status = BackgroundTask.STATUS_ERROR
            task.output = e.messages
        except Exception as e:
            task.status = BackgroundTask.STATUS_ERROR
            task.output = str(e)
        finally:
            task.finished = timezone.now()
            task.save(update_fields=["status", "finished", "output"])

    def duplicate_data(self, message: dict):
        """Duplicate the data and update task status."""
        # Break circular import.
        from resolwe.flow.models import Data

        def duplicate():
            duplicates = bulk_duplicate(
                data=Data.objects.filter(pk__in=message["data_ids"]),
                contributor=get_user_model().objects.get(pk=message["contributor_id"]),
                inherit_entity=message["inherit_entity"],
                inherit_collection=message["inherit_collection"],
            )
            return list(duplicates.values_list("pk", flat=True))

        self.wrap_task(duplicate, BackgroundTask.objects.get(pk=message["task_id"]))

    def duplicate_entity(self, message: dict):
        """Duplicate the entities and update task status."""
        # Break circular import.
        from resolwe.flow.models import Entity

        def duplicate():
            duplicates = bulk_duplicate(
                entities=Entity.objects.filter(pk__in=message["entity_ids"]),
                contributor=get_user_model().objects.get(pk=message["contributor_id"]),
                inherit_collection=message["inherit_collection"],
            )
            return list(duplicates.values_list("pk", flat=True))

        self.wrap_task(duplicate, BackgroundTask.objects.get(pk=message["task_id"]))

    def duplicate_collection(self, message: dict):
        """Duplicate the collections and update task status."""
        # Break circular import.
        from resolwe.flow.models import Collection

        def duplicate():
            duplicates = bulk_duplicate(
                collections=Collection.objects.filter(pk__in=message["collection_ids"]),
                contributor=get_user_model().objects.get(pk=message["contributor_id"]),
            )
            return list(duplicates.values_list("pk", flat=True))

        self.wrap_task(duplicate, BackgroundTask.objects.get(pk=message["task_id"]))
