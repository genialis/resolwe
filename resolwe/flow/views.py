"""
==========
Flow Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil
from importlib import import_module

from django.db import IntegrityError
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

from .models import Project, Process, Data, DescriptorSchema, Trigger, Storage
from .serializers import (ProjectSerializer, ProcessSerializer, DataSerializer,
                          DescriptorSchemaSerializer, TriggerSerializer, StorageSerializer)


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


permissions_cls = load_permissions(settings.FLOW['API']['PERMISSIONS'])


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

        request.data['contributor'] = user.pk
        try:
            return super(ResolweCreateModelMixin, self).create(request, *args, **kwargs)
        except IntegrityError as ex:
            return Response({u'error': str(ex)}, status=status.HTTP_409_CONFLICT)


class ResolweCreateDataModelMixin(ResolweCreateModelMixin):
    """Mixin to support creating new :class:`Data` objects

    Extends :class:`ResolweCcreateModelMixin` with:

      * checks if there is exactly 1 project listed on create
      * checks if user has `add` permission on that project

    """
    def create(self, request, *args, **kwargs):
        projects = request.data.get('projects', [])
        if len(projects) != 1:
            return Response({'projects': 'Exactly one id required on create.'},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            project = Project.objects.get(pk=projects[0])
        except Project.DoesNotExist:
            return Response({'projects': ['Invalid pk "{}" - object does not exist.'.format(projects[0])]},
                            status=status.HTTP_400_BAD_REQUEST)

        if not request.user.has_perm('add_project', obj=project):
            if request.user.is_authenticated():
                raise exceptions.PermissionDenied
            else:
                raise exceptions.NotFound

        return super(ResolweCreateDataModelMixin, self).create(request, *args, **kwargs)


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

        if 'projects' in data:
            if 'add' in data['projects']:
                for _id in data['projects']['add']:
                    try:
                        Project.objects.get(pk=_id).public_processes.add(obj)
                        # obj.projects.add(Project.objects.get(pk=_id))
                    except Project.DoesNotExist:
                        pass
            if 'remove' in data['projects']:
                for _id in data['projects']['remove']:
                    try:
                        Project.objects.get(pk=_id).public_processes.remove(obj)
                        # obj.projects.remove(Project.objects.get(pk=_id))
                    except Project.DoesNotExist:
                        pass


class ProjectViewSet(ResolweCreateModelMixin,
                     mixins.RetrieveModelMixin,
                     mixins.UpdateModelMixin,
                     mixins.DestroyModelMixin,
                     mixins.ListModelMixin,
                     ResolwePermissionsMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Project` objects."""

    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    permission_classes = (permissions_cls,)


class ProcessViewSet(mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     ResolweProcessPermissionsMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Process` objects."""

    queryset = Process.objects.all()
    serializer_class = ProcessSerializer
    permission_classes = (permissions_cls,)


class DataViewSet(ResolweCreateDataModelMixin,
                  mixins.RetrieveModelMixin,
                  mixins.UpdateModelMixin,
                  mixins.DestroyModelMixin,
                  mixins.ListModelMixin,
                  ResolwePermissionsMixin,
                  viewsets.GenericViewSet):

    """API view for :class:`Data` objects."""

    queryset = Data.objects.all()
    serializer_class = DataSerializer
    permission_classes = (permissions_cls,)


class DescriptorSchemaViewSet(mixins.RetrieveModelMixin,
                              mixins.ListModelMixin,
                              ResolwePermissionsMixin,
                              viewsets.GenericViewSet):

    """API view for :class:`DescriptorSchema` objects."""

    queryset = DescriptorSchema.objects.all()
    serializer_class = DescriptorSchemaSerializer
    permission_classes = (permissions_cls,)


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


class StorageViewSet(mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     viewsets.GenericViewSet):

    """API view for :class:`Storage` objects."""

    queryset = Storage.objects.all()
    serializer_class = StorageSerializer
