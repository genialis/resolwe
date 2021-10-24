"""Permissions functions used in Resolwe Viewsets."""
from collections import defaultdict

from django.db import models, transaction

from rest_framework import exceptions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.permissions.models import Permission, get_anonymous_user
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

            def _permission_mapping(serializer_self):
                """Return mapping between the permission group and permissions.

                We must not filter the queryset as permission data has already
                been prefetched.
                """
                if not hasattr(self, "permission_map"):
                    self.anonymous_user = get_anonymous_user()
                    self.permission_map = defaultdict(list)

                    if isinstance(serializer_self.instance, models.Model):
                        instances = [serializer_self.instance]
                    else:
                        instances = serializer_self.instance

                    for instance in instances:
                        for permission in instance.permission_group.permissions.all():
                            self.permission_map[permission.permission_group_id].append(
                                (
                                    permission.user or permission.group,
                                    permission.permission,
                                    "user" if permission.user else "group",
                                )
                            )
                return self.permission_map

            def to_representation(serializer_self, instance):
                """Object serializer."""

                def user_data(user, permission):
                    if user == self.anonymous_user:
                        return {
                            "type": "public",
                            "permissions": [str(perm) for perm in list(permission)],
                        }
                    return {
                        "type": "user",
                        "id": user.pk,
                        "name": user.get_full_name() or user.username,
                        "username": user.username,
                        "permissions": [str(perm) for perm in list(permission)],
                    }

                def group_data(group, permission):
                    return {
                        "type": "group",
                        "id": group.pk,
                        "name": group.name,
                        "permissions": [str(perm) for perm in list(permission)],
                    }

                get_perm_data = {"user": user_data, "group": group_data}
                permission_map = serializer_self._permission_mapping()
                data = super().to_representation(instance)

                if (
                    "fields" not in self.request.query_params
                    or "current_user_permissions" in self.request.query_params["fields"]
                ):
                    data["current_user_permissions"] = [
                        get_perm_data[entity_type](entity, permission)
                        for entity, permission, entity_type in permission_map[
                            instance.permission_group_id
                        ]
                    ]
                return data

        return SerializerWithPermissions

    @action(
        detail=True,
        methods=["get", "post"],
        url_path="permissions",
        url_name="permissions",
    )
    def detail_permissions(self, request, pk=None):
        """Get or set permissions API endpoint."""
        # This object comes frot the queryset which permissions are prefetched
        # for current user only. This implies that
        # obj.permission_group.permissions returns only permissions fot the
        # current user. We have to perform the refresh for permissions to work
        # correctly. If not it is impossible to remove the permission for
        # non-logged in users as they are not seen.
        obj = self.get_object()
        obj.refresh_from_db(fields=["permission_group"])

        if request.method == "POST":
            payload = request.data
            user = request.user
            is_owner = user.has_perm(Permission.OWNER, obj=obj)
            allow_owner = is_owner or user.is_superuser
            check_owner_permission(payload, allow_owner)
            check_public_permissions(payload)
            check_user_permissions(payload, request.user.pk)

            with transaction.atomic():
                update_permission(obj, payload)
                owner_count = obj.permission_group.permissions.filter(
                    value=Permission.OWNER.value, user__isnull=False
                ).count()

                if not owner_count:
                    raise exceptions.ParseError("Object must have at least one owner.")

        return Response(get_object_perms(obj))

    @action(
        detail=False,
        methods=["get", "post"],
        url_path="permissions",
        url_name="permissions",
    )
    def list_permissions(self, request):
        """Batch get or set permissions API endpoint."""
        # TODO: Implement batch get/set permissions
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)
