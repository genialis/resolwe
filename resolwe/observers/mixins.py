"""Mixins for Observable ViewSets."""

from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError
from drf_spectacular.utils import extend_schema
from rest_framework import serializers
from rest_framework.decorators import action
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response

from resolwe.permissions.models import get_anonymous_user

from .models import Observer, Subscription
from .protocol import ChangeType


class ObserverSubscribeRequestSerializer(serializers.Serializer):
    """Serializer for the subscribe endpoint."""

    ids = serializers.ListField(
        child=serializers.IntegerField(), required=False, allow_null=True
    )
    session_id = serializers.CharField()


class ObserverSubscriptionSerializer(serializers.Serializer):
    """Serializer for the subscribe response."""

    subscription_id = serializers.UUIDField()


class ObservableMixin:
    """A Mixin to make a model ViewSet observable.

    Add the subscribe and unsubscribe endpoints to the list view.
    """

    def user_has_permission(self, id: int, user: User) -> bool:
        """Verify that an object exists for a given user."""
        return self.get_queryset().filter(pk=id).filter_for_user(user).exists()

    @extend_schema(
        request=ObserverSubscribeRequestSerializer,
        responses={200: ObserverSubscriptionSerializer},
    )
    @action(detail=False, methods=["post"])
    def subscribe(self, request: Request) -> Response:
        """Register an Observer for a resource."""
        deserializer = ObserverSubscribeRequestSerializer(data=request.data)
        deserializer.is_valid(raise_exception=True)
        ids = deserializer.validated_data.get("ids")
        session_id = deserializer.validated_data["session_id"]

        content_type = ContentType.objects.get_for_model(self.get_queryset().model)
        user = request.user if request.user.is_authenticated else get_anonymous_user()
        subscription = Subscription.objects.create(user=user, session_id=session_id)

        if ids is None:
            # Subscribe to the whole table.
            object_ids = [Observer.ALL_IDS]
            change_types = (ChangeType.CREATE, ChangeType.DELETE)
        else:
            # Verify all ids exists and user has permissions to view them.
            for id in ids:
                if not self.user_has_permission(id, request.user):
                    raise NotFound(f"Item {id} does not exist")

            object_ids = ids
            change_types = (ChangeType.UPDATE, ChangeType.DELETE, ChangeType.CREATE)

        try:
            subscription.subscribe(content_type, object_ids, change_types)
        except IntegrityError:
            # When the websocket of the session closes while subscribing, its
            # disconnect handler deletes the new subscription and adding the
            # next observer to it violates the foreign key. Each observer is added
            # in its own transaction, so the error is raised here and not when the
            # request commits.
            if Subscription.objects.filter(pk=subscription.pk).exists():
                raise
            raise NotFound(f"Session {session_id} was closed.")

        serializer = ObserverSubscriptionSerializer(
            {"subscription_id": subscription.subscription_id}
        )
        return Response(serializer.data)

    @extend_schema(request=ObserverSubscriptionSerializer, responses={200: None})
    @action(detail=False, methods=["post"])
    def unsubscribe(self, request: Request) -> Response:
        """Unregister a subscription."""
        deserializer = ObserverSubscriptionSerializer(data=request.data)
        deserializer.is_valid(raise_exception=True)
        subscription_id = deserializer.validated_data["subscription_id"]

        user = request.user if request.user.is_authenticated else get_anonymous_user()
        subscription = Subscription.objects.filter(
            subscription_id=subscription_id, user=user
        ).first()

        if subscription is None:
            raise NotFound(
                f"Subscription {subscription_id} for user {user} does not exist."
            )

        subscription.delete()
        return Response()
