"""
==========
Apps Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import viewsets

from .models import Package, App
from .serializers import AppSerializer, PackageSerializer


class PackageViewSet(viewsets.ModelViewSet):

    """API view for Package objects."""

    queryset = Package.objects.all()
    serializer_class = PackageSerializer


class AppViewSet(viewsets.ModelViewSet):

    """API view for App objects."""

    queryset = App.objects.all()
    serializer_class = AppSerializer
