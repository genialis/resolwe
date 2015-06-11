"""
==========
Flow Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import viewsets

from resolwe.flow.models import Project, Tool, Data, AnnotationSchema, Trigger, Storage
from resolwe.flow.permissions import ResolwePermissions
from resolwe.flow.serializers import (ProjectSerializer, ToolSerializer, DataSerializer,
                                      AnnotationSchemaSerializer, TriggerSerializer, StorageSerializer)


class ProjectViewSet(viewsets.ModelViewSet):

    """API view for Project objects."""

    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    permission_classes = (ResolwePermissions,)


class ToolViewSet(viewsets.ModelViewSet):

    """API view for Tool objects."""

    queryset = Tool.objects.all()
    serializer_class = ToolSerializer
    permission_classes = (ResolwePermissions,)


class DataViewSet(viewsets.ModelViewSet):

    """API view for Data objects."""

    queryset = Data.objects.all()
    serializer_class = DataSerializer
    permission_classes = (ResolwePermissions,)


class AnnotationSchemaViewSet(viewsets.ModelViewSet):

    """API view for AnnotationSchema objects."""

    queryset = AnnotationSchema.objects.all()
    serializer_class = AnnotationSchemaSerializer
    permission_classes = (ResolwePermissions,)


class TriggerViewSet(viewsets.ModelViewSet):

    """API view for Trigger objects."""

    queryset = Trigger.objects.all()
    serializer_class = TriggerSerializer
    permission_classes = (ResolwePermissions,)


class StorageViewSet(viewsets.ModelViewSet):

    """API view for Storage objects."""

    queryset = Storage.objects.all()
    serializer_class = StorageSerializer
