"""Permissions functions used in Resolwe Viewsets."""
from collections import defaultdict
from typing import Optional

from django.conf import settings
from django.db import models, transaction

from rest_framework import exceptions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.permissions.models import Permission
from resolwe.permissions.shortcuts import get_object_perms, get_user_group_perms_qset

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

            def __init__(
                serializer_self,
                instance: Optional[models.QuerySet] = None,
                *args,
                **kwargs,
            ):
                """Override init."""
                # print("Instance")
                # print(instance, type(instance), instance.all())
                if isinstance(instance, models.Model):
                    query_set = instance._meta.model.objects.filter(pk=instance.pk)
                # elif isinstance(
                #     instance, (models.QuerySet, models.query.EmptyQuerySet)
                # ):
                else:
                    query_set = instance
                # elif instance is not None:
                #     raise RuntimeError(
                #         f"Instance {instance} must be a QuerySet or Model."
                #     )
                if instance is not None:
                    serializer_self._permissions_qset = get_user_group_perms_qset(
                        self.request.user, query_set
                    )
                serializer_self._permission_map = None
                super().__init__(instance, *args, **kwargs)

            def get_fields(serializer_self):
                """Return serializer's fields."""
                fields = super().get_fields()
                fields["current_user_permissions"] = CurrentUserPermissionsSerializer(
                    read_only=True
                )
                return fields

            def _set_permission_map(serializer_self):
                """Set the permission_map property."""
                if serializer_self._permission_map is None:
                    serializer_self._permission_map = defaultdict(list)
                    for value in serializer_self._permissions_qset:
                        entity_type = "user" if value["is_user"] else "group"
                        if (
                            entity_type == "user"
                            and value["entityname"] == settings.ANONYMOUS_USER_NAME
                        ):
                            entity_type = "public"
                        serializer_self._permission_map[value["pk"]].append(
                            {
                                "type": entity_type,
                                "id": value["entityid"],
                                "name": value["entityname"],
                                "permissions": Permission(
                                    value["permission"]
                                ).all_permissions(),
                            }
                        )

            def create(serializer_self, validated_data):
                """Override create.

                After instance is created from validated data construct
                permissions queryset and store it.
                """
                instance = super().create(validated_data)
                if isinstance(instance, models.Model):
                    query_set = instance._meta.model.objects.filter(pk=instance.pk)
                elif isinstance(instance, models.QuerySet):
                    query_set = instance
                if instance is not None:
                    serializer_self._permissions_qset = get_user_group_perms_qset(
                        self.request.user, query_set
                    )
                return instance

            def to_representation(serializer_self, instance: models.Model):
                """Object serializer."""
                data = super().to_representation(instance)
                if (
                    "fields" not in self.request.query_params
                    or "current_user_permissions" in self.request.query_params["fields"]
                ):
                    serializer_self._set_permission_map()
                    # data["current_user_permissions"] = get_object_perms(
                    #    instance, self.request.user
                    # )
                    data["current_user_permissions"] = serializer_self._permission_map[
                        instance.pk
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
        # print("Detail permission"*1000)
        obj = self.get_object()

        if request.method == "POST":
            payload = request.data
            user = request.user
            is_owner = user.has_perm("owner", obj=obj)
            allow_owner = is_owner or user.is_superuser
            check_owner_permission(payload, allow_owner)
            check_public_permissions(payload)
            check_user_permissions(payload, request.user.pk)

            with transaction.atomic():
                update_permission(obj, payload)
                owner_permission = obj.permission_group.permissions.filter(
                    permission=Permission.from_name("owner").value
                ).first()
                owner_count = 0
                if owner_permission is not None:
                    owner_count = owner_permission.users.count()

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
