"""Consumers for Observers."""

import asyncio
import logging
from enum import Enum
from typing import Callable, Optional, TypedDict

from channels.consumer import AsyncConsumer
from channels.db import database_sync_to_async
from channels.generic.websocket import JsonWebsocketConsumer
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone

from resolwe.auditlog.logger import logger as audit_logger
from resolwe.flow.models.utils.duplicate import (
    bulk_duplicate_collection,
    bulk_duplicate_data,
    bulk_duplicate_entity,
)

from .models import BackgroundTask, Observer, Subscription
from .protocol import GROUP_SESSIONS, ChangeType, ChannelsMessage, WebsocketMessage

# The channel used to listen for BackgrountTask events
BACKGROUND_TASK_CHANNEL = "observers.background_task"

logger = logging.getLogger(__name__)


class MoveMessage(TypedDict):
    """The message used when moving objects between collections."""

    task_id: int
    target_id: int
    data_ids: list[int]
    entity_ids: list[int]
    request_user_id: int


class BackgroundTaskType(Enum):
    """Background task types."""

    DUPLICATE_DATA = "duplicate_data"
    DUPLICATE_ENTITY = "duplicate_entity"
    DUPLICATE_COLLECTION = "duplicate_collection"
    DELETE = "delete"
    MOVE = "move_between_collections"


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
                return task

    async def duplicate_data(self, message: dict):
        """Duplicate the data and update task status."""
        # Break circular import.
        from resolwe.flow.models import Data

        def duplicate():
            duplicates = bulk_duplicate_data(
                data=Data.objects.filter(pk__in=message["data_ids"]),
                contributor=get_user_model().objects.get(pk=message["request_user_id"]),
                inherit_entity=message["inherit_entity"],
                inherit_collection=message["inherit_collection"],
            )
            return [duplicate.pk for duplicate in duplicates]

        task = await self.wrap_task(duplicate, message["task_id"])
        if task is not None and task.status == BackgroundTask.STATUS_DONE:
            audit_logger.info(
                "User %s duplicated data objects with ids %s.",
                message["request_user_id"],
                message["data_ids"],
            )

    async def duplicate_entity(self, message: dict):
        """Duplicate the entities and update task status."""
        # Break circular import.
        from resolwe.flow.models import Entity

        def duplicate():
            duplicates = bulk_duplicate_entity(
                entities=Entity.objects.filter(pk__in=message["entity_ids"]),
                contributor=get_user_model().objects.get(pk=message["request_user_id"]),
                inherit_collection=message["inherit_collection"],
            )
            return [duplicate.pk for duplicate in duplicates]

        task = await self.wrap_task(duplicate, message["task_id"])
        if task is not None and task.status == BackgroundTask.STATUS_DONE:
            audit_logger.info(
                "User %s duplicated entity objects with ids %s.",
                message["request_user_id"],
                message["entity_ids"],
            )

    async def duplicate_collection(self, message: dict):
        """Duplicate the collections and update task status."""
        # Break circular import.
        from resolwe.flow.models import Collection

        def duplicate():
            duplicates = bulk_duplicate_collection(
                Collection.objects.filter(pk__in=message["collection_ids"]),
                contributor=get_user_model().objects.get(pk=message["request_user_id"]),
            )
            return [duplicate.pk for duplicate in duplicates]

        task = await self.wrap_task(duplicate, message["task_id"])
        if task is not None and task.status == BackgroundTask.STATUS_DONE:
            audit_logger.info(
                "User %s duplicated collection objects with ids %s.",
                message["request_user_id"],
                message["collection_ids"],
            )

    async def move_between_collections(self, message: MoveMessage):
        """Move data objects and entities between two collections.

        Only one type of objects can be moved at the same time.
        """

        # Break circular import.
        from resolwe.flow.models import Collection, Data, Entity

        @transaction.atomic
        def move():
            target = Collection.objects.get(pk=message["target_id"])
            entities = [
                Entity.objects.get(pk=entity_id) for entity_id in message["entity_ids"]
            ]
            data_objects = [
                Data.objects.get(pk=data_id) for data_id in message["data_ids"]
            ]
            assert (
                not entities or not data_objects
            ), "Can not move entities and data objects at the same time."
            for datum in data_objects or entities:
                datum.move_to_collection(target)

        task = await self.wrap_task(move, message["task_id"])
        if task is not None and task.status == BackgroundTask.STATUS_DONE:
            objects_type = "entities" if message["entity_ids"] else "data objects"
            object_ids = message["entity_ids"] or message["data_ids"]
            audit_logger.info(
                "User %s moved %s with ids %s to collection %s.",
                message["request_user_id"],
                objects_type,
                object_ids,
                message["target_id"],
            )

    async def delete(self, message: dict):
        """Delete the objects and update task status.

        :param message: the message containing the following keys:
            - ``task_id``: the id of the background task
            - ``content_type_id``: the content type of the model to be deleted
            - ``object_ids``: the ids of the objects to be deleted
        """

        def delete():
            to_delete = Model.objects.filter(pk__in=message["object_ids"])

            # Use iterator and delete one object at a time to trigger the signals.
            for obj in to_delete.iterator():
                obj.delete()

        @database_sync_to_async_new_thread
        def get_model():
            """Get the model from the content type id."""
            return ContentType.objects.get_for_id(
                message["content_type_id"]
            ).model_class()

        Model = await get_model()
        task = await self.wrap_task(delete, message["task_id"])
        if task is not None and task.status == BackgroundTask.STATUS_DONE:
            audit_logger.info(
                "User %s deleted %s objects with ids %s.",
                message["request_user_id"],
                Model._meta.verbose_name,
                message["object_ids"],
            )
