"""
==========
Flow Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil
from importlib import import_module

from django.db import IntegrityError, transaction
from django.db.models import Q
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ImproperlyConfigured
from django.utils._os import upath


from rest_framework import exceptions, mixins, viewsets, status
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from guardian import shortcuts

from .filters import DataFilter, CollectionFilter, ProcessFilter
from .managers import manager
from .models import Collection, Process, Data, DescriptorSchema, Trigger, Storage
from .serializers import (CollectionSerializer, ProcessSerializer, DataSerializer,
                          DescriptorSchemaSerializer, TriggerSerializer, StorageSerializer)

from resolwe.permissions.shortcuts import get_object_perms


def assign_perm(*args, **kwargs):
    """Wrapper for assign_perm function

    Call original assign_perms function from django-guardian, but don't
    raise exception if permission is not found

    """
    try:
        shortcuts.assign_perm(*args, **kwargs)
    except Permission.DoesNotExist:
        pass


def remove_perm(*args, **kwargs):
    """Wrapper for remove_perm function

    Call original remove_perms function from django-guardian, but don't
    raise exception if permission is not found

    """
    try:
        shortcuts.remove_perm(*args, **kwargs)
    except Permission.DoesNotExist:
        pass


def load_permissions(permissions_name):
    """Look for a fully qualified flow permissions class."""
    try:
        return import_module('{}'.format(permissions_name)).ResolwePermissions
    except AttributeError:
        raise AttributeError("'ResolwePermissions' class not found in {} module.".format(
                             permissions_name))
    except ImportError as ex:
        # The permissions module wasn't found. Display a helpful error
        # message listing all possible (built-in) permissions classes.
        permissions_dir = os.path.join(os.path.dirname(upath(__file__)), '..', 'perms')
        permissions_dir = os.path.normpath(permissions_dir)

        try:
            builtin_permissions = [
                name for _, name, _ in pkgutil.iter_modules([permissions_dir]) if name not in [u'tests']]
        except EnvironmentError:
            builtin_permissions = []
        if permissions_name not in ['resolwe.auth.{}'.format(p) for p in builtin_permissions]:
            permissions_reprs = map(repr, sorted(builtin_permissions))
            err_msg = ("{} isn't an available flow permissions class.\n"
                       "Try using 'resolwe.auth.XXX', where XXX is one of:\n"
                       "    {}\n"
                       "Error was: {}".format(permissions_name, ", ".join(permissions_reprs), ex))
            raise ImproperlyConfigured(err_msg)
        else:
            # If there's some other error, this must be an error in Django
            raise


permissions_cls = load_permissions(settings.FLOW_API['PERMISSIONS'])


class ResolweCreateModelMixin(mixins.CreateModelMixin):
    """Mixin to support creating new `Resolwe` models

    Extends `django_rest_framework`'s class `CreateModelMixin` with:

      * append user's id from request to posted data as `contributor`
        key
      * catch `IntegrityError`s, so we can return HTTP status 409
        instead of raising error

    """
    def create(self, request, *args, **kwargs):
        user = request.user
        if not user.is_authenticated():
            raise exceptions.NotFound

        ds_slug = request.data.get('descriptor_schema', None)
        if ds_slug:
            ds_query = DescriptorSchema.objects.filter(slug=ds_slug).order_by('version')
            if not ds_query.exists():
                return Response(
                    {'descriptor_schema':
                        ['Invalid descriptor_schema slug "{}" - object does not exist.'.format(ds_slug)]},
                    status=status.HTTP_400_BAD_REQUEST)
            request.data['descriptor_schema'] = ds_query.last().pk

        request.data['contributor'] = user.pk
        try:
            return super(ResolweCreateModelMixin, self).create(request, *args, **kwargs)

        except IntegrityError as ex:
            return Response({u'error': str(ex)}, status=status.HTTP_409_CONFLICT)

    def perform_create(self, serializer):
        with transaction.atomic():
            instance = serializer.save()

            # Assign all permissions to the object contributor.
            for permission in list(zip(*instance._meta.permissions))[0]:
                assign_perm(permission, instance.contributor, instance)


class ResolweCreateDataModelMixin(ResolweCreateModelMixin):
    """Mixin to support creating new :class:`Data` objects

    Extends :class:`ResolweCcreateModelMixin` with:

      * checks if there is exactly 1 collection listed on create
      * checks if user has `add` permission on that collection

    """
    def create(self, request, *args, **kwargs):
        collections = request.data.get('collections', [])

        for collection_id in collections:
            try:
                collection = Collection.objects.get(pk=collection_id)
            except Collection.DoesNotExist:
                return Response({'collections': ['Invalid pk "{}" - object does not exist.'.format(collection_id)]},
                                status=status.HTTP_400_BAD_REQUEST)

            if not request.user.has_perm('add_collection', obj=collection):
                if request.user.is_authenticated():
                    raise exceptions.PermissionDenied
                else:
                    raise exceptions.NotFound

        process_slug = request.data.get('process', None)
        process_query = Process.objects.filter(slug=process_slug).order_by('version')
        if not process_query.exists():
            # XXX: security - is it ok to reveal which processes (don't) exist?
            return Response({'process': ['Invalid process slug "{}" - object does not exist.'.format(process_slug)]},
                            status=status.HTTP_400_BAD_REQUEST)
        process = process_query.last()
        request.data['process'] = process.pk

        if not request.user.has_perm('view_process', obj=process):
            if request.user.is_authenticated():
                raise exceptions.PermissionDenied
            else:
                raise exceptions.NotFound

        resp = super(ResolweCreateDataModelMixin, self).create(request, *args, **kwargs)
        manager.communicate()
        return resp

    def perform_create(self, serializer):
        with transaction.atomic():
            instance = serializer.save()

            # Assign all permissions to the object contributor.
            for permission in list(zip(*instance._meta.permissions))[0]:
                assign_perm(permission, instance.contributor, instance)

        collections = self.request.data.get('collections', [])
        for c in collections:
            collection = Collection.objects.get(pk=c)
            collection.data.add(instance)


class ResolwePermissionsMixin(object):
    """Mixin to support managing `Resolwe` objects' permissions.



    """

    def _fetch_user(self, query):
        """Get user by ``pk`` or ``username``. Return ``None`` if doesn't exist."""
        User = get_user_model()

        user_filter = {'pk': query} if query.isdigit() else {'username': query}
        try:
            return User.objects.get(**user_filter)
        except User.DoesNotExist:
            raise exceptions.ParseError("User ({}) does not exists.".format(user_filter))

    def _fetch_group(self, query):
        """Get group by ``pk`` or ``name``. Return ``None`` if doesn't exist."""

        group_filter = {'pk': query} if query.isdigit() else {'name': query}
        try:
            return Group.objects.get(**group_filter)
        except Group.DoesNotExist:
            raise exceptions.ParseError("Group ({}) does not exists.".format(group_filter))

    def _update_permission(self, obj, data):
        content_type = ContentType.objects.get_for_model(obj)
        full_permissions = list(zip(*obj._meta.permissions))[0]

        def set_permissions(entity_type, perm_type):
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
            perm_func = assign_perm if perm_type == 'add' else remove_perm
            user = AnonymousUser()
            perms = data.get('public', {}).get(perm_type, [])
            if perms == u'ALL':
                perms = full_permissions
            for perm in perms:
                perm_func('{}_{}'.format(perm.lower(), content_type), user, obj)

        set_public_permissions('add')
        set_public_permissions('remove')

    def get_serializer_class(self):
        # Augment the base serializer class to include permissions information with objects.
        base_class = super(ResolwePermissionsMixin, self).get_serializer_class()

        class SerializerWithPermissions(base_class):
            def to_representation(serializer_self, instance):  # pylint: disable=no-self-argument
                # TODO: These permissions queries may be expensive. Should we limit or optimize this?
                data = super(SerializerWithPermissions, serializer_self).to_representation(instance)
                data['permissions'] = get_object_perms(instance, self.request.user)
                return data

        return SerializerWithPermissions

    def _filter_owner_permission(self, data):
        """Raise ``PermissionDenied``if ``owner`` found in ``data``"""
        for entity_type in ['users', 'groups']:
            if entity_type in data:
                for perm_type in ['add', 'remove']:
                    if perm_type in data[entity_type]:
                        for entity_id in data[entity_type][perm_type]:
                            for perm in data[entity_type][perm_type][entity_id]:
                                if perm == 'owner':
                                    raise exceptions.PermissionDenied("Only owners can grant/revoke owner permission")

    def _filter_public_permissions(self, data):
        """Raise ``PermissionDenied`` if public permissions are too open"""

        ALLOWED_PUBLIC_PERMISSIONS = ['view', 'add', 'download']

        if 'public' in data:
            for perm_type in ['add', 'remove']:
                if perm_type in data['public']:
                    for perm in data['public'][perm_type]:
                        if perm not in ALLOWED_PUBLIC_PERMISSIONS:
                            raise exceptions.PermissionDenied("Permissions for public users are too open")

    def _filter_user_permissions(self, data, user_pk):
        """Raise ``PermissionDenied`` if ``data`` includes ``user_pk``"""
        if 'users' in data:
            for perm_type in ['add', 'remove']:
                if perm_type in data['users']:
                    if user_pk in data['users'][perm_type].keys():
                        raise exceptions.PermissionDenied("You cannot change your own permissions")

    @detail_route(methods=['get', 'post'], url_path='permissions')
    def detail_permissions(self, request, pk=None):
        """API endpoint to get/set permissions"""
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
        # TODO
        return Response(status=status.HTTP_501_NOT_IMPLEMENTED)


class ResolweProcessPermissionsMixin(ResolwePermissionsMixin):

    def _update_permission(self, obj, data):
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


class ResolweCheckSlugMixin(object):
    @list_route(methods=[u'get'])
    def slug_exists(self, request):
        """Check if given url slug exists.

        Check if slug given in query parameter ``name`` exists. Return
        ``True`` if slug already exists and ``False`` otherwise.

        """
        if not request.user.is_authenticated():
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        if 'name' not in request.query_params:
            return Response({'error': 'Query parameter `name` must be given.'},
                            status=status.HTTP_400_BAD_REQUEST)

        queryset = self.get_queryset()
        slug_name = request.query_params['name']
        return Response(queryset.filter(slug__iexact=slug_name).exists())


class CollectionViewSet(ResolweCreateModelMixin,
                        mixins.RetrieveModelMixin,
                        mixins.UpdateModelMixin,
                        mixins.DestroyModelMixin,
                        mixins.ListModelMixin,
                        ResolwePermissionsMixin,
                        ResolweCheckSlugMixin,
                        viewsets.GenericViewSet):

    """API view for :class:`Collection` objects."""

    queryset = Collection.objects.all().prefetch_related('descriptor_schema')
    serializer_class = CollectionSerializer
    permission_classes = (permissions_cls,)
    filter_class = CollectionFilter

    @detail_route(methods=[u'post'])
    def add_data(self, request, pk=None):
        collection = self.get_object()

        if not request.user.has_perm('add_collection', obj=collection):
            if request.user.is_authenticated():
                raise exceptions.PermissionDenied()
            else:
                raise exceptions.NotFound()

        if 'ids' not in request.data:
            return Response({"error": "`ids`parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        missing = []
        for data_id in request.data['ids']:
            if not Data.objects.filter(pk=data_id).exists():
                missing.append(data_id)

        if missing:
            return Response(
                {"error": "Data objects with following ids are missing: {}".format(', '.join(missing))},
                status=status.HTTP_400_BAD_REQUEST)

        for data_id in request.data['ids']:
            collection.data.add(data_id)

        return Response()

    @detail_route(methods=[u'post'])
    def remove_data(self, request, pk=None):
        collection = self.get_object()

        if not request.user.has_perm('add_collection', obj=collection):
            if request.user.is_authenticated():
                raise exceptions.PermissionDenied()
            else:
                raise exceptions.NotFound()

        if 'ids' not in request.data:
            return Response({"error": "`ids`parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for data_id in request.data['ids']:
            collection.data.remove(data_id)

        return Response()


class ProcessViewSet(ResolweCreateModelMixin,
                     mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     ResolweProcessPermissionsMixin,
                     ResolweCheckSlugMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Process` objects."""

    queryset = Process.objects.all()
    serializer_class = ProcessSerializer
    permission_classes = (permissions_cls,)
    filter_class = ProcessFilter


class DataViewSet(ResolweCreateDataModelMixin,
                  mixins.RetrieveModelMixin,
                  mixins.UpdateModelMixin,
                  mixins.DestroyModelMixin,
                  mixins.ListModelMixin,
                  ResolwePermissionsMixin,
                  ResolweCheckSlugMixin,
                  viewsets.GenericViewSet):

    """API view for :class:`Data` objects."""

    queryset = Data.objects.all().prefetch_related('process', 'descriptor_schema')
    serializer_class = DataSerializer
    permission_classes = (permissions_cls,)
    filter_class = DataFilter


class DescriptorSchemaViewSet(mixins.RetrieveModelMixin,
                              mixins.ListModelMixin,
                              ResolwePermissionsMixin,
                              viewsets.GenericViewSet):

    """API view for :class:`DescriptorSchema` objects."""

    queryset = DescriptorSchema.objects.all()
    serializer_class = DescriptorSchemaSerializer
    permission_classes = (permissions_cls,)
    filter_fields = ('contributor', 'name', 'description', 'created', 'modified', 'slug')


class TriggerViewSet(ResolweCreateModelMixin,
                     mixins.RetrieveModelMixin,
                     mixins.UpdateModelMixin,
                     mixins.DestroyModelMixin,
                     mixins.ListModelMixin,
                     ResolwePermissionsMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Trigger` objects."""

    queryset = Trigger.objects.all()
    serializer_class = TriggerSerializer
    permission_classes = (permissions_cls,)
    filter_fields = ('contributor', 'name', 'created', 'modified', 'slug', 'collection')


class StorageViewSet(mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Storage` objects."""

    queryset = Storage.objects.all()
    serializer_class = StorageSerializer
    filter_fields = ('contributor', 'name', 'created', 'modified', 'slug', 'json')
