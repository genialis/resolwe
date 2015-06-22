"""
==========
Flow Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pkgutil
from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils._os import upath

from rest_framework import viewsets

from .models import Project, Tool, Data, AnnotationSchema, Trigger, Storage
from .serializers import (ProjectSerializer, ToolSerializer, DataSerializer,
                          AnnotationSchemaSerializer, TriggerSerializer, StorageSerializer)


def get_permissions(permissions_name):
    """Look for a fully qualified flow permissions class."""
    try:
        return import_module('{}'.format(permissions_name)).ResolwePermissions
    except AttributeError:
        raise AttributeError("'ResolwePermissions' class not found in {} module.".format(
                             permissions_name))
    except ImportError as ex:
        # The permissions module wasn't found. Display a helpful error
        # message listing all possible (built-in) permissions classes.
        permissions_dir = os.path.join(os.path.dirname(upath(__file__)), '..', 'permissions')
        permissions_dir = os.path.normpath(permissions_dir)

        try:
            builtin_permissions = [name for _, name, _ in pkgutil.iter_modules([permissions_dir])]
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


permissions_cls = get_permissions(settings.FLOW['API']['AUTHORIZATION'])


class ProjectViewSet(viewsets.ModelViewSet):

    """API view for Project objects."""

    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    permission_classes = (permissions_cls,)


class ToolViewSet(viewsets.ModelViewSet):

    """API view for Tool objects."""

    queryset = Tool.objects.all()
    serializer_class = ToolSerializer
    permission_classes = (permissions_cls,)


class DataViewSet(viewsets.ModelViewSet):

    """API view for Data objects."""

    queryset = Data.objects.all()
    serializer_class = DataSerializer
    permission_classes = (permissions_cls,)


class AnnotationSchemaViewSet(viewsets.ModelViewSet):

    """API view for AnnotationSchema objects."""

    queryset = AnnotationSchema.objects.all()
    serializer_class = AnnotationSchemaSerializer
    permission_classes = (permissions_cls,)


class TriggerViewSet(viewsets.ModelViewSet):

    """API view for Trigger objects."""

    queryset = Trigger.objects.all()
    serializer_class = TriggerSerializer
    permission_classes = (permissions_cls,)


class StorageViewSet(viewsets.ModelViewSet):

    """API view for Storage objects."""

    queryset = Storage.objects.all()
    serializer_class = StorageSerializer
