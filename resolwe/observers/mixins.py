"""Mixins for Observable ViewSets."""
from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType

from rest_framework.decorators import action
from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response

from resolwe.permissions.models import get_anonymous_user

from .models import Subscription
from .protocol import ChangeType


class ObservableMixin:
    """A Mixin to make a model ViewSet observable.

    Add the subscribe and unsubscribe endpoints to the list view.
    """

    def user_has_permission(self, id: int, user: User) -> bool:
        """Verify that an object exists for a given user."""
        return self.get_queryset().filter(pk=id).filter_for_user(user).exists()

    @action(detail=False, methods=["post"])
    def subscribe(self, request: Request) -> Response:
        """Register an Observer for a resource."""
        ids = request.data.get("ids", None)
        session_id = request.data.get("session_id")
        content_type = ContentType.objects.get_for_model(self.get_queryset().model)
        user = request.user if request.user.is_authenticated else get_anonymous_user()
        subscription = Subscription.objects.create(user=user, session_id=session_id)

        if ids is None:
            # Subscribe to the whole table.
            subscription.subscribe(content_type, [None], (ChangeType.CREATE,))
        else:
            # Verify all ids exists and user has permissions to view them.
            for id in ids:
                if not self.user_has_permission(id, request.user):
                    raise NotFound(f"Item {id} does not exist")

            change_types = (ChangeType.UPDATE, ChangeType.DELETE)
            subscription.subscribe(content_type, ids, change_types)

        resp = {"subscription_id": subscription.subscription_id}
        return Response(resp)

    @action(detail=False, methods=["post"])
    def unsubscribe(self, request: Request) -> Response:
        """Unregister a subscription."""
        subscription_id = request.data.get("subscription_id", None)
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
