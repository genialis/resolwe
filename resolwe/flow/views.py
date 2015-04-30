"""
==========
Flow Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import viewsets

from .models import Project, Tool, Data, AnnotationSchema, Trigger, Storage
from .serializers import (ProjectSerializer, ToolSerializer, DataSerializer, AnnotationSchemaSerializer,
                          TriggerSerializer, StorageSerializer)


class ProjectViewSet(viewsets.ModelViewSet):

    """API view for Project objects."""

    queryset = Project.objects.all()
    serializer_class = ProjectSerializer


class ToolViewSet(viewsets.ModelViewSet):

    """API view for Tool objects."""

    queryset = Tool.objects.all()
    serializer_class = ToolSerializer


class DataViewSet(viewsets.ModelViewSet):

    """API view for Data objects."""

    queryset = Data.objects.all()
    serializer_class = DataSerializer


class AnnotationSchemaViewSet(viewsets.ModelViewSet):

    """API view for AnnotationSchema objects."""

    queryset = AnnotationSchema.objects.all()
    serializer_class = AnnotationSchemaSerializer


class TriggerViewSet(viewsets.ModelViewSet):

    """API view for Trigger objects."""

    queryset = Trigger.objects.all()
    serializer_class = TriggerSerializer


class StorageViewSet(viewsets.ModelViewSet):

    """API view for Storage objects."""

    queryset = Storage.objects.all()
    serializer_class = StorageSerializer
