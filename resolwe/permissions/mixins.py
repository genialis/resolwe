"""Permissions functions used in Resolwe Viewsets."""
from __future__ import absolute_import, division, print_function, unicode_literals

from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

from django.contrib.contenttypes.models import ContentType

from rest_framework import status
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from resolwe.permissions.shortcuts import get_object_perms

from .utils import check_owner_permission, check_public_permissions, check_user_permissions, update_permission


class ResolwePermissionsMixin(object):
    """Mixin to support managing `Resolwe` objects' permissions."""

    def get_serializer_class(self):
        """Augment base serializer class.

        Include permissions information with objects.

        """
        base_class = super(ResolwePermissionsMixin, self).get_serializer_class()

        class SerializerWithPermissions(base_class):
            """Augment serializer class."""

            def to_representation(serializer_self, instance):  # pylint: disable=no-self-argument
                """Object serializer."""
                # TODO: These permissions queries may be expensive. Should we limit or optimize this?
                data = super(SerializerWithPermissions, serializer_self).to_representation(instance)
                data['permissions'] = get_object_perms(instance, self.request.user)
                return data

        return SerializerWithPermissions

    def set_content_permissions(self, user, obj, payload):
        """Overwritte this function in sub-classes if needed.

        It will be called if ``share_content`` query parameter will be
        passed with the request. So it should take care of sharing
        content of current object, i.e. data objects and samples
        attached to a collection.
        """
        pass

    @detail_route(methods=['get', 'post'], url_path='permissions')
    def detail_permissions(self, request, pk=None):
        """Get or set permissions API endpoint."""
        obj = self.get_object()

        if request.method == 'POST':
            content_type = ContentType.objects.get_for_model(obj)
            payload = request.data
            share_content = strtobool(payload.pop('share_content', 'false'))
            user = request.user
            is_owner = user.has_perm('owner_{}'.format(content_type), obj=obj)

            if not (is_owner or user.is_superuser):
                check_owner_permission(payload)
            check_public_permissions(payload)
            check_user_permissions(payload, request.user.pk)

            update_permission(obj, payload)

            if share_content:
                self.set_content_permissions(user, obj, payload)

        return Response(get_object_perms(obj))

    @list_route(methods=['get', 'post'], url_path='permissions')
    def list_permissions(self, request):
        """Batch get or set permissions API endpoint."""
        # TODO: Implement batch get/set permissions
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)
