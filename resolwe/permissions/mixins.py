"""Permissions functions used in Resolwe Viewsets."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.contenttypes.models import ContentType

from guardian.shortcuts import assign_perm, remove_perm
from rest_framework import exceptions, status
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from resolwe.flow.models import Collection
from resolwe.permissions.shortcuts import get_object_perms


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

    def _fetch_user(self, query):
        """Get user by ``pk`` or ``username``. Return ``None`` if doesn't exist."""
        user_model = get_user_model()

        user_filter = {'pk': query} if query.isdigit() else {'username': query}
        try:
            return user_model.objects.get(**user_filter)
        except user_model.DoesNotExist:
            raise exceptions.ParseError("User ({}) does not exists.".format(user_filter))

    def _fetch_group(self, query):
        """Get group by ``pk`` or ``name``. Return ``None`` if doesn't exist."""
        group_filter = {'pk': query} if query.isdigit() else {'name': query}
        try:
            return Group.objects.get(**group_filter)
        except Group.DoesNotExist:
            raise exceptions.ParseError("Group ({}) does not exists.".format(group_filter))

    def _update_permission(self, obj, data):
        """Update object permissions."""
        content_type = ContentType.objects.get_for_model(obj)
        full_permissions = list(zip(*obj._meta.permissions))[0]  # pylint: disable=protected-access

        def set_permissions(entity_type, perm_type):
            """Set object permissions."""
            perm_func = assign_perm if perm_type == 'add' else remove_perm
            fetch = self._fetch_user if entity_type == 'users' else self._fetch_group

            for entity_id in data.get(entity_type, {}).get(perm_type, []):
                entity = fetch(entity_id)
                if entity:
                    perms = data[entity_type][perm_type][entity_id]
                    if perms == u'ALL':
                        perms = full_permissions
                    for perm in perms:
                        perm_func('{}_{}'.format(perm.lower(), content_type), entity, obj)

        set_permissions('users', 'add')
        set_permissions('users', 'remove')
        set_permissions('groups', 'add')
        set_permissions('groups', 'remove')

        def set_public_permissions(perm_type):
            """Set public permissions."""
            perm_func = assign_perm if perm_type == 'add' else remove_perm
            user = AnonymousUser()
            perms = data.get('public', {}).get(perm_type, [])
            if perms == u'ALL':
                perms = full_permissions
            for perm in perms:
                perm_func('{}_{}'.format(perm.lower(), content_type), user, obj)

        set_public_permissions('add')
        set_public_permissions('remove')

    def _filter_owner_permission(self, data):
        """Raise ``PermissionDenied``if ``owner`` found in ``data``."""
        for entity_type in ['users', 'groups']:
            if entity_type in data:
                for perm_type in ['add', 'remove']:
                    if perm_type in data[entity_type]:
                        for entity_id in data[entity_type][perm_type]:
                            for perm in data[entity_type][perm_type][entity_id]:
                                if perm == 'owner':
                                    raise exceptions.PermissionDenied("Only owners can grant/revoke owner permission")

    def _filter_public_permissions(self, data):
        """Raise ``PermissionDenied`` if public permissions are too open."""
        allowed_public_permissions = ['view', 'add', 'download']

        if 'public' in data:
            for perm_type in ['add', 'remove']:
                if perm_type in data['public']:
                    for perm in data['public'][perm_type]:
                        if perm not in allowed_public_permissions:
                            raise exceptions.PermissionDenied("Permissions for public users are too open")

    def _filter_user_permissions(self, data, user_pk):
        """Raise ``PermissionDenied`` if ``data`` includes ``user_pk``."""
        if 'users' in data:
            for perm_type in ['add', 'remove']:
                if perm_type in data['users']:
                    if user_pk in data['users'][perm_type].keys():
                        raise exceptions.PermissionDenied("You cannot change your own permissions")

    @detail_route(methods=['get', 'post'], url_path='permissions')
    def detail_permissions(self, request, pk=None):
        """Get or set permissions API endpoint."""
        obj = self.get_object()

        if request.method == 'POST':
            content_type = ContentType.objects.get_for_model(obj)

            owner_perm = 'owner_{}'.format(content_type)
            if not (request.user.has_perm(owner_perm, obj=obj) or request.user.is_superuser):
                self._filter_owner_permission(request.data)
            self._filter_public_permissions(request.data)
            self._filter_user_permissions(request.data, request.user.pk)

            self._update_permission(obj, request.data)

        return Response(get_object_perms(obj))

    @list_route(methods=['get', 'post'], url_path='permissions')
    def list_permissions(self, request):
        """Batch get or set permissions API endpoint."""
        # TODO: Implement batch get/set permissions
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)


class ResolweProcessPermissionsMixin(ResolwePermissionsMixin):
    """Process permissions mixin."""

    def _update_permission(self, obj, data):
        """Update collection permissions."""
        super(ResolweProcessPermissionsMixin, self)._update_permission(obj, data)

        if 'collections' in data:
            if 'add' in data['collections']:
                for _id in data['collections']['add']:
                    try:
                        Collection.objects.get(pk=_id).public_processes.add(obj)
                    except Collection.DoesNotExist:
                        pass
            if 'remove' in data['collections']:
                for _id in data['collections']['remove']:
                    try:
                        Collection.objects.get(pk=_id).public_processes.remove(obj)
                    except Collection.DoesNotExist:
                        pass
