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
import rest_framework_filters as filters

from .managers import manager
from .models import Collection, Process, Data, DescriptorSchema, Trigger, Storage
from .serializers import (CollectionSerializer, ProcessSerializer, DataSerializer,
                          DescriptorSchemaSerializer, TriggerSerializer, StorageSerializer)

from resolwe.permissions.shortcuts import get_user_group_perms


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
        ds_query = DescriptorSchema.objects.filter(slug=ds_slug).order_by('version')
        if ds_slug and not ds_query.exists():
            return Response(
                {'descriptor_schema': ['Invalid descriptor_schema slug "{}" - object does not exist.'.format(ds_slug)]},
                status=status.HTTP_400_BAD_REQUEST)

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

        ds_slug = self.request.data.get('descriptor_schema', None)
        if ds_slug:
            descriptor_schema = DescriptorSchema.objects.filter(slug=ds_slug).order_by('version').last()
            instance.descriptor_schema = descriptor_schema
            instance.save()


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

        ds_slug = request.data.get('descriptor_schema', None)
        ds_query = DescriptorSchema.objects.filter(slug=ds_slug).order_by('version')
        if ds_slug and not ds_query.exists():
            return Response(
                {'descriptor_schema':
                    ['Invalid descriptor_schema slug "{}" - object does not exist.'.format(ds_slug)]},
                status=status.HTTP_400_BAD_REQUEST)

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

        ds_slug = self.request.data.get('descriptor_schema', None)
        if ds_slug:
            descriptor_schema = DescriptorSchema.objects.filter(slug=ds_slug).order_by('version').last()
            instance.descriptor_schema = descriptor_schema
            instance.save()


class ResolwePermissionsMixin(object):
    """Mixin to support managing `Resolwe` objects' permissions.



    """
    def filter_public_permisions(self, perms):
        """Return list of parameters applicable for public user.

        :param list perms: List of permissions to filter
        :return: List of parameters
        :rtype: list

        """
        return [perm for perm in perms if perm.startswith('view') or perm.startswith('download')]

    def _fetch_user(self, query):
        try:
            return get_user_model().objects.get(Q(pk=query) | Q(email=query))
        except get_user_model().DoesNotExist:
            return None

    def _fetch_group(self, query):
        try:
            return Group.objects.get(Q(pk=query) | Q(name=query))
        except Group.DoesNotExist:
            return None

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
            perms = self.filter_public_permisions(perms)
            for perm in perms:
                perm_func('{}_{}'.format(perm.lower(), content_type), user, obj)

        set_public_permissions('add')
        set_public_permissions('remove')

    def _get_object_permissions(self, user, instance):
        def format_permissions(perms):
            return [perm.replace('_{}'.format(instance._meta.model_name), '') for perm in perms]

        if user.is_authenticated():
            permissions_user, permissions_group = get_user_group_perms(user, instance)
        else:
            permissions_user, permissions_group = [], []
        permissions_public = shortcuts.get_perms(AnonymousUser(), instance)

        return {
            'user': format_permissions(permissions_user),
            'group': format_permissions(permissions_group),
            'public': format_permissions(permissions_public),
        }

    def get_serializer_class(self):
        # Augment the base serializer class to include permissions information with objects.
        base_class = super(ResolwePermissionsMixin, self).get_serializer_class()

        class SerializerWithPermissions(base_class):
            def to_representation(serializer, instance):
                # TODO: These permissions queries may be expensive. Should we limit or optimize this?
                data = super(SerializerWithPermissions, serializer).to_representation(instance)
                data['permissions'] = self._get_object_permissions(self.request.user, instance)
                return data

        return SerializerWithPermissions

    @detail_route(methods=[u'post'], url_path='permissions')
    def detail_permissions(self, request, pk=None):
        obj = self.get_object()
        content_type = ContentType.objects.get_for_model(obj)

        if not request.user.has_perm('share_{}'.format(content_type), obj=obj):
            if request.user.is_authenticated():
                raise exceptions.PermissionDenied()
            else:
                raise exceptions.NotFound()

        self._update_permission(obj, request.data)

        # resp = UserObjectPermission.objects.filter(object_pk=obj.pk)
        return Response()

    @list_route(methods=[u'post'], url_path='permissions')
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
                        # obj.collections.add(Collection.objects.get(pk=_id))
                    except Collection.DoesNotExist:
                        pass
            if 'remove' in data['collections']:
                for _id in data['collections']['remove']:
                    try:
                        Collection.objects.get(pk=_id).public_processes.remove(obj)
                        # obj.collections.remove(Collection.objects.get(pk=_id))
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


class DescriptorSchemaFilter(filters.FilterSet):
    class Meta:
        model = DescriptorSchema
        fields = {
            'slug': filters.ALL_LOOKUPS,
        }


class CollectionFilter(filters.FilterSet):
    data = filters.ModelChoiceFilter(queryset=Data.objects.all())
    descriptor_schema = filters.RelatedFilter(DescriptorSchemaFilter, name='descriptor_schema')

    class Meta:
        model = Collection
        fields = ('contributor', 'name', 'description', 'created', 'modified',
                  'slug', 'descriptor', 'data', 'descriptor_schema', 'id')


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


class ProcessFilter(filters.FilterSet):
    category = filters.CharFilter(name='category', lookup_type='startswith')

    class Meta:
        model = Process
        fields = ('contributor', 'name', 'created', 'modified', 'slug', 'category')


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


class DataFilter(filters.FilterSet):
    collection = filters.ModelChoiceFilter(queryset=Collection.objects.all())
    type = filters.CharFilter(name='process__type', lookup_type='startswith')

    class Meta:
        model = Data
        fields = ('contributor', 'name', 'created', 'modified', 'slug', 'input', 'descriptor',
                  'started', 'finished', 'output', 'status', 'process', 'type', 'collection')


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
