"""The model Observer model."""

import uuid
from time import sleep, time
from typing import Any, Iterable, List, Optional, Set, Tuple

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import models, transaction
from django.db.models import Count, Q
from django.db.models.query import QuerySet

from resolwe.flow.models.base import BaseManagerWithoutVersion
from resolwe.permissions.models import Permission, PermissionObject

from .protocol import GROUP_SESSIONS, TYPE_ITEM_UPDATE, ChangeType, ChannelsMessage

# Type alias for observable object.
Observable = PermissionObject


def _default_output_value():
    """Return the default value for output field in the BackgroundTask model.

    Must be named method due to Django bug in migrations.
    """
    return ""


class BackgroundTask(Observable):
    """The observable model representing the background task.

    When a long running task is started, the instance of type BackgroundTask is created
    and frontend can subscribe to receive its notifications.
    """

    #: background task is waiting
    STATUS_WAITING = "WT"
    #: background task is processing
    STATUS_PROCESSING = "PR"
    #: background task has finished successfully
    STATUS_DONE = "OK"
    #: background task has finished with error
    STATUS_ERROR = "ER"

    STATUS_CHOICES = (
        (STATUS_WAITING, "Waiting"),
        (STATUS_PROCESSING, "Processing"),
        (STATUS_DONE, "Done"),
        (STATUS_ERROR, "Error"),
    )

    objects = BaseManagerWithoutVersion()

    #: task start date and time
    started = models.DateTimeField(blank=True, null=True, db_index=True)

    #: task finished date and time
    finished = models.DateTimeField(blank=True, null=True, db_index=True)

    #: status of the background task
    status = models.CharField(
        max_length=2, choices=STATUS_CHOICES, default=STATUS_WAITING
    )

    #: task description
    description = models.CharField(max_length=256)

    #: the field containing output from the backgroud task, such as list of ids of
    #: duplicated objects, error details...
    output = models.JSONField(default=_default_output_value)

    def wait(
        self,
        timeout: float = 2,
        final_statuses: list[str] = [STATUS_DONE],
        polling_interval: float = 0.3,
    ):
        """Wait for up to timeout seconds for task to transition into final_statuses.

        The method is meant to be used in tests as it creates lots of database queries.

        :raises RuntimeError: when desired status was not reached within timeout.
        """
        started = time()
        while self.status not in final_statuses and time() - started < timeout:
            sleep(polling_interval)
            self.refresh_from_db()

        if self.status not in final_statuses:
            raise RuntimeError(
                f"Task failed to reach {final_statuses} within {timeout} seconds."
            )

    def result(
        self,
        timeout: float = 2,
        final_statuses: list[str] = [STATUS_DONE],
        polling_interval: float = 0.3,
    ) -> Any:
        """Return the output field of the background task when finished.

        The method waits for up to timeout seconds for task to reach the one of
        final_statuses and returns its result (output field).

        :raises RuntimeError: when desired status was not reached within the timeout.
        """
        self.wait(timeout, final_statuses, polling_interval)
        return self.output


def get_random_uuid() -> str:
    """Generate a random UUID in string format."""
    return uuid.uuid4().hex


class Observer(models.Model):
    """An observer for detecting changes in a model.

    Characterized by a db table or an instance of a model and a change type.
    Several subscriptions can subscribe to the same observer.
    """

    ALL_IDS = 0  # The constant used for catchall object id.
    CHANGE_TYPES = [(change.value, change.name) for change in ChangeType]

    #: table of the observed resource
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    #: primary key of the observed resource (null if watching the whole table)
    object_id = models.PositiveIntegerField(null=False)
    #: the type of change to observe for
    change_type = models.PositiveSmallIntegerField(choices=CHANGE_TYPES)

    class Meta:
        """Add index to session_id field and set defining fields."""

        indexes = [models.Index(fields=["object_id"])]
        unique_together = ("content_type", "object_id", "change_type")

    @classmethod
    def get_interested(
        cls,
        content_type: ContentType,
        object_id: int,
        change_type: Optional[ChangeType] = None,
    ) -> "QuerySet[Observer]":
        """Find all observers watching for changes of a given item/table."""
        query = Q(content_type=content_type)
        if change_type is not None:
            query &= Q(change_type=change_type.value)
        query &= Q(object_id=object_id) | Q(object_id=Observer.ALL_IDS)
        return cls.objects.filter(query)

    @classmethod
    def observe_instance_container(
        cls,
        instance: Observable,
        change_type: ChangeType,
        containers: Optional[Iterable[Observable]] = None,
    ):
        """Handle a notifications to the containers of the given instance.

        When containers are given notify them else infer them from the instance.
        """
        # Test explicitely for None, since containers may be empty.
        if containers is None:
            containers = instance.containers
        for container in containers:
            observers = Observer.get_interested(
                change_type=change_type,
                content_type=ContentType.objects.get_for_model(container),
                object_id=container.pk,
            )
            # Forward the message to the appropriate groups.
            for subscriber in Subscription.objects.filter(observers__in=observers):
                if container.has_permission(Permission.VIEW, subscriber.user):
                    # Register on_commit callbacks to send the signals.
                    Subscription.notify(
                        subscriber.session_id,
                        container,
                        change_type,
                        source=(
                            ContentType.objects.get_for_model(instance).name,
                            instance.pk,
                        ),
                    )

    @classmethod
    def observe_instance_changes(cls, instance: Observable, change_type: ChangeType):
        """Handle a notification about an instance change."""
        content_type = ContentType.objects.get_for_model(instance)
        observers = Observer.get_interested(
            change_type=change_type,
            content_type=content_type,
            object_id=instance.pk,
        )

        # Forward the message to the appropriate groups.
        for subscriber in Subscription.objects.filter(observers__in=observers):
            if instance.has_permission(Permission.VIEW, subscriber.user):
                # Register on_commit callbacks to send the signals.
                Subscription.notify(
                    subscriber.session_id,
                    instance,
                    change_type,
                    source=(content_type.name, instance.pk),
                )

    @classmethod
    def observe_permission_changes(
        cls,
        instance: Any,
        gains: Set[int],
        losses: Set[int],
        observe_containers: bool = True,
    ):
        """Handle a notification about a permission change.

        Given an instance and a set of user_ids who gained/lost permissions for it,
        only relevant observers will be notified of the instance's creation/deletion.

        When observe_containers is set to True (default), also object's containers are
        notified using the same change type.
        """
        for change_type, user_ids in (
            (ChangeType.CREATE, gains),
            (ChangeType.DELETE, losses),
        ):
            # A shortcut if nothing actually changed.
            if len(user_ids) == 0:
                continue

            # Find all sessions who have observers registered on this object.
            content_type = ContentType.objects.get_for_model(instance)
            interested = Observer.get_interested(
                change_type=change_type,
                content_type=content_type,
                object_id=instance.pk,
            )
            # Of all interested users, select only those whose permissions changed.
            session_ids = set(
                Subscription.objects.filter(observers__in=interested)
                .filter(user__in=user_ids)
                .values_list("session_id", flat=True)
                .distinct()
            )

            for session_id in session_ids:
                Subscription.notify(
                    session_id,
                    instance,
                    change_type,
                    source=(content_type.name, instance.pk),
                )

            if observe_containers:
                cls.observe_instance_container(instance, change_type)

    def __str__(self) -> str:
        """Format the object representation."""
        return f"content_type={self.content_type} object_id={self.object_id} change={self.change_type}"


class Subscription(models.Model):
    """Subscription to several observers.

    One subscription corresponds to a single api call to the subscribe endpoint.
    A client may have several subscriptions with different ID's to differentiate
    between them.
    """

    #: observers to whom the subscription is listening
    observers = models.ManyToManyField("Observer", related_name="subscriptions")
    #: subscriber's user reference
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
    )
    #: subscription time
    created = models.DateTimeField(auto_now_add=True)

    #: ID of the websocket session (one session may have multiple observers)
    session_id = models.CharField(max_length=100)
    #: unique ID for the client to remember which subscription a signal belongs to
    subscription_id = models.UUIDField(
        unique=True, default=get_random_uuid, editable=False
    )

    class Meta:
        """Add index to session_id field."""

        indexes = [models.Index(fields=["session_id"])]

    def subscribe(
        self,
        content_type: ContentType,
        object_ids: List[int],
        change_types: List[ChangeType],
    ):
        """Assign self to multiple observers at once."""
        for id in object_ids:
            for change_type in change_types:
                observer, _ = Observer.objects.get_or_create(
                    content_type=content_type,
                    object_id=id,
                    change_type=change_type.value,
                )
                self.observers.add(observer)

    def delete(self):
        """Delete the given subscription.

        Delete all observers with no remaining subscriptions.
        """
        # Find related observers with only one remaining subscription
        # (it must be this one) and delete them first.
        Observer.objects.annotate(subs=Count("subscriptions")).filter(
            subscriptions=self.pk, subs=1
        ).delete()
        super().delete()

    @classmethod
    def notify(
        cls,
        session_id: str,
        instance: Observable,
        change_type: ChangeType,
        source: Optional[Tuple[str, int]],
    ):
        """Register a callback to send a change notification on transaction commit."""
        notification: ChannelsMessage = {
            "type": TYPE_ITEM_UPDATE,
            "content_type_pk": ContentType.objects.get_for_model(instance).pk,
            "change_type_value": change_type.value,
            "object_id": instance.pk,
            "source": source,
        }

        # Define a callback, but copy variable values.
        def trigger(
            channel_layer=get_channel_layer(),
            channel=GROUP_SESSIONS.format(session_id=session_id),
            notification=notification,
        ):
            async_to_sync(channel_layer.group_send)(channel, notification)

        transaction.on_commit(trigger)

    def notify_created(self, content_type: ContentType):
        """Send a create notification.

        Used to send signal when models without permissions are created.
        """
        notification: ChannelsMessage = {
            "type": TYPE_ITEM_UPDATE,
            "content_type_pk": content_type.pk,
            "change_type_value": ChangeType.CREATE.value,
            "object_id": Observer.ALL_IDS,
            "source": None,
        }
        channel_layer = get_channel_layer()
        channel = GROUP_SESSIONS.format(session_id=self.session_id)
        async_to_sync(channel_layer.group_send)(channel, notification)
