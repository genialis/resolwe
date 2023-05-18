"""Consumers for Observers."""

import asyncio
import logging
from enum import Enum
from typing import Callable, Optional

from channels.consumer import AsyncConsumer
from channels.db import database_sync_to_async
from channels.generic.websocket import JsonWebsocketConsumer

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.utils import timezone

from resolwe.flow.models.utils import bulk_duplicate

from .models import BackgroundTask, Observer, Subscription
from .protocol import GROUP_SESSIONS, ChangeType, ChannelsMessage, WebsocketMessage

logger = logging.getLogger(__name__)
# The channel used to listen for BackgrountTask events
BACKGROUND_TASK_CHANNEL = "observers.background_task"


class BackgroundTaskType(Enum):
    """Background task types."""

    DUPLICATE_DATA = "duplicate_data"
    DUPLICATE_ENTITY = "duplicate_entity"
    DUPLICATE_COLLECTION = "duplicate_collection"
    DELETE = "delete"


def update_constants():
    """Recreate channel name constant with changed settings.

    This kludge is mostly needed due to the way Django settings are
    patched for testing and how modules need to be imported throughout
    the project. On import time, settings are not patched yet, but some
    of the code needs static values immediately. Updating functions such
    as this one are then needed to fix dummy values.
    """
    global BACKGROUND_TASK_CHANNEL
    redis_prefix = getattr(settings, "FLOW_MANAGER", {}).get("REDIS_PREFIX", "")

    BACKGROUND_TASK_CHANNEL = f"{redis_prefix}.observers.background_task"


update_constants()


def database_sync_to_async_new_thread(*args, **kwargs):
    """Patched database_sync_to_async decorator.

    That sets thread_sensitive to True by default, otherwise the defalut executor tends
    to lock.
    """
    target_arg_name = "thread_sensitive"
    # Make sure not to override explicitely set flag.
    if target_arg_name not in kwargs:
        kwargs[target_arg_name] = False
    return database_sync_to_async(*args, **kwargs)


class ClientConsumer(JsonWebsocketConsumer):
    """Consumer for client communication."""

    def websocket_connect(self, event: dict[str, str]):
        """Handle establishing a WebSocket connection."""
        session_id: Optional[str] = self.scope["url_route"]["kwargs"].get(
            "session_id", None
        )

        # Close and log the requests without a session_id.
        if session_id is None:
            logger.warning("Closing websocket connection request without session_id.")
            self.close()
        else:
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


class BackgroundTaskConsumer(AsyncConsumer):
    """The background task consumer."""

    @database_sync_to_async_new_thread
    def wrap_task(self, function: Callable, task_id: int):
        """Start the function and update background task status."""
        task: Optional[BackgroundTask] = None
        try:
            task = BackgroundTask.objects.get(pk=task_id)
            task.started = timezone.now()
            task.status = BackgroundTask.STATUS_PROCESSING
            task.save(update_fields=["status", "started"])
            task.output = function() or "Task completed."
            task.status = BackgroundTask.STATUS_DONE
        # Task may not exist here if the consumer was cancelled duging the creation
        # of the task. Only proceed if it is defined.
        except ValidationError as e:
            if task:
                task.status = BackgroundTask.STATUS_ERROR
                task.output = e.messages
        except asyncio.CancelledError:
            if task:
                task.status = BackgroundTask.STATUS_ERROR
                task.output = "Task was cancelled."
        except Exception as e:
            if task:
                task.status = BackgroundTask.STATUS_ERROR
                task.output = str(e)
        finally:
            if task:
                task.finished = timezone.now()
                task.save(update_fields=["status", "finished", "output"])

    async def duplicate_data(self, message: dict):
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

        await self.wrap_task(duplicate, message["task_id"])

    async def duplicate_entity(self, message: dict):
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

        await self.wrap_task(duplicate, message["task_id"])

    async def duplicate_collection(self, message: dict):
        """Duplicate the collections and update task status."""
        # Break circular import.
        from resolwe.flow.models import Collection

        def duplicate():
            duplicates = bulk_duplicate(
                collections=Collection.objects.filter(pk__in=message["collection_ids"]),
                contributor=get_user_model().objects.get(pk=message["contributor_id"]),
            )
            return list(duplicates.values_list("pk", flat=True))

        await self.wrap_task(duplicate, message["task_id"])

    async def delete(self, message: dict):
        """Delete the objects and update task status.

        :param message: the message containing the following keys:
            - ``task_id``: the id of the background task
            - ``content_type_id``: the content type of the model to be deleted
            - ``object_ids``: the ids of the objects to be deleted
        """

        def delete():
            Model = ContentType.objects.get_for_id(
                message["content_type_id"]
            ).model_class()
            to_delete = Model.objects.filter(pk__in=message["object_ids"])

            # Use iterator and delete one object at a time to trigger the signals.
            for obj in to_delete.iterator():
                obj.delete()

        await self.wrap_task(delete, message["task_id"])
