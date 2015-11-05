"""
==========
Apps Views
==========

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.conf import settings

from rest_framework import mixins, viewsets

from ..flow.views import load_permissions, ResolwePermissionsMixin, ResolweCreateModelMixin
from .models import Package, App
from .serializers import AppSerializer, PackageSerializer


permissions_cls = load_permissions(settings.FLOW_API['PERMISSIONS'])


class PackageViewSet(mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     ResolwePermissionsMixin,
                     viewsets.GenericViewSet):

    """API view for Package objects."""

    queryset = Package.objects.all()
    serializer_class = PackageSerializer
    permission_classes = (permissions_cls,)
    filter_fields = ('contributor', 'name', 'created', 'modified', 'slug')


class AppViewSet(ResolweCreateModelMixin,
                 mixins.RetrieveModelMixin,
                 mixins.UpdateModelMixin,
                 mixins.DestroyModelMixin,
                 mixins.ListModelMixin,
                 ResolwePermissionsMixin,
                 viewsets.GenericViewSet):

    """API view for App objects."""

    queryset = App.objects.all()
    serializer_class = AppSerializer
    permission_classes = (permissions_cls,)
    filter_fields = ('contributor', 'name', 'created', 'modified', 'slug', 'package')
