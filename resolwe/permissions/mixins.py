"""Permissions functions used in Resolwe Viewsets."""
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from guardian.models import UserObjectPermission
from rest_framework import exceptions, serializers, status
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.permissions.shortcuts import get_object_perms

from .utils import check_owner_permission, check_public_permissions, check_user_permissions, update_permission


class CurrentUserPermissionsSerializer(serializers.Serializer):  # pylint: disable=abstract-method
    """Current user permissions serializer."""

    id = serializers.IntegerField()  # pylint: disable=invalid-name
    type = serializers.CharField(max_length=50)
    name = serializers.CharField(max_length=100)
    permissions = serializers.ListField(
        child=serializers.CharField(max_length=30)
    )


class ResolwePermissionsMixin:
    """Mixin to support managing `Resolwe` objects' permissions."""

    def get_serializer_class(self):
        """Augment base serializer class.

        Include permissions information with objects.

        """
        base_class = super().get_serializer_class()

        class SerializerWithPermissions(base_class):
            """Augment serializer class."""

            def get_fields(serializer_self):  # pylint: disable=no-self-argument
                """Return serializer's fields."""
                fields = super().get_fields()
                fields['current_user_permissions'] = CurrentUserPermissionsSerializer(read_only=True)
                return fields

            def to_representation(serializer_self, instance):  # pylint: disable=no-self-argument
                """Object serializer."""
                data = super().to_representation(instance)

                if ('fields' not in self.request.query_params
                        or 'current_user_permissions' in self.request.query_params['fields']):
                    data['current_user_permissions'] = get_object_perms(instance, self.request.user)

                return data

        return SerializerWithPermissions

    def set_content_permissions(self, user, obj, payload):
        """Overwritte this function in sub-classes if needed.

        It will be called if ``share_content`` query parameter will be
        passed with the request. So it should take care of sharing
        content of current object, i.e. data objects and samples
        attached to a collection.
        """

    @action(detail=True, methods=['get', 'post'], url_path='permissions', url_name='permissions')
    def detail_permissions(self, request, pk=None):
        """Get or set permissions API endpoint."""
        obj = self.get_object()

        if request.method == 'POST':
            content_type = ContentType.objects.get_for_model(obj)
            payload = request.data
            share_content = strtobool(payload.pop('share_content', 'false'))
            user = request.user
            is_owner = user.has_perm('owner_{}'.format(content_type), obj=obj)

            allow_owner = is_owner or user.is_superuser
            check_owner_permission(payload, allow_owner)
            check_public_permissions(payload)
            check_user_permissions(payload, request.user.pk)

            with transaction.atomic():
                update_permission(obj, payload)

                owner_count = UserObjectPermission.objects.filter(
                    object_pk=obj.id,
                    content_type=content_type,
                    permission__codename__startswith='owner_'
                ).count()

                if not owner_count:
                    raise exceptions.ParseError('Object must have at least one owner.')

            if share_content:
                self.set_content_permissions(user, obj, payload)

        return Response(get_object_perms(obj))

    @action(detail=False, methods=['get', 'post'], url_path='permissions', url_name='permissions')
    def list_permissions(self, request):
        """Batch get or set permissions API endpoint."""
        # TODO: Implement batch get/set permissions
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)
