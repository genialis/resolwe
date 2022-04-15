"""Permissions functions used in Resolwe Viewsets."""
from django.conf import settings
from django.db import models, transaction

from rest_framework import exceptions, serializers, status
from rest_framework.decorators import action
from rest_framework.request import Request
from rest_framework.response import Response

from resolwe.auditlog.auditmanager import AuditManager
from resolwe.permissions.models import Permission
from resolwe.permissions.shortcuts import get_object_perms

from .utils import (
    check_owner_permission,
    check_public_permissions,
    check_user_permissions,
    update_permission,
)


class CurrentUserPermissionsSerializer(serializers.Serializer):
    """Current user permissions serializer."""

    id = serializers.IntegerField()
    type = serializers.CharField(max_length=50)
    name = serializers.CharField(max_length=100)
    permissions = serializers.ListField(child=serializers.CharField(max_length=30))


class ResolwePermissionsMixin:
    """Mixin to support managing `Resolwe` objects' permissions."""

    def prefetch_current_user_permissions(self, queryset: models.QuerySet):
        """Prefetch permissions for the current user."""
        user = self.request.user
        filters = models.Q(user__username=settings.ANONYMOUS_USER_NAME)
        if not user.is_anonymous:
            filters |= models.Q(user=user) | models.Q(group__in=user.groups.all())

        qs_permission_model = self.qs_permission_model.filter(filters)
        return queryset.prefetch_related(
            models.Prefetch(
                "permission_group__permissions", queryset=qs_permission_model
            )
        )

    def get_serializer_class(self):
        """Augment base serializer class.

        Include permissions information with objects.

        """
        base_class = super().get_serializer_class()

        class SerializerWithPermissions(base_class):
            """Augment serializer class."""

            def get_fields(serializer_self):
                """Return serializer's fields."""
                fields = super().get_fields()
                fields["current_user_permissions"] = CurrentUserPermissionsSerializer(
                    read_only=True
                )
                return fields

            def to_representation(serializer_self, instance: models.Model):
                """Object serializer."""
                data = super().to_representation(instance)
                if (
                    "fields" not in self.request.query_params
                    or "current_user_permissions" in self.request.query_params["fields"]
                ):
                    data["current_user_permissions"] = get_object_perms(
                        instance, self.request.user, True
                    )
                return data

        return SerializerWithPermissions

    @action(
        detail=True,
        methods=["get", "post"],
        url_path="permissions",
        url_name="permissions",
    )
    def detail_permissions(self, request: Request, pk=None) -> Response:
        """Get or set permissions API endpoint."""
        # The object is taken from the queryset on the view for which
        # permissions are prefetched for the current user only.
        # This implies that obj.permission_group.permissions returns
        # permissions for the current user.
        # To get all the permissions we have to perform a refresh from the
        # database. This must only be done if user has share permission on the
        # given object otherwise only his permissions must be returned.
        obj = self.get_object()

        if obj.has_permission(Permission.SHARE, request.user):
            obj.refresh_from_db()

        audit_manager = AuditManager.global_instance()

        if request.method == "POST":
            audit_manager.log_message("Permissions updated: %s", request.data)
            allow_owner = obj.is_owner(request.user) or request.user.is_superuser
            check_owner_permission(request.data, allow_owner, obj)
            check_public_permissions(request.data)
            check_user_permissions(request.data, request.user.pk)

            with transaction.atomic():
                update_permission(obj, request.data)
                owner_count = obj.permission_group.permissions.filter(
                    value=Permission.OWNER.value, user__isnull=False
                ).count()

                if not owner_count:
                    raise exceptions.ParseError("Object must have at least one owner.")
        else:
            audit_manager.log_message("Permissions read: %s", request.data)

        return Response(get_object_perms(obj))

    @action(
        detail=False,
        methods=["get", "post"],
        url_path="permissions",
        url_name="permissions",
    )
    def list_permissions(self, request: Request) -> Response:
        """Batch get or set permissions API endpoint."""
        # TODO: Implement batch get/set permissions
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)
