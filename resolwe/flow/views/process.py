"""Process viewset."""
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import mixins, viewsets

from resolwe.flow.filters import ProcessFilter
from resolwe.flow.models import Process
from resolwe.flow.serializers import ProcessSerializer
from resolwe.permissions.loader import permissions_cls
from resolwe.permissions.mixins import ResolweProcessPermissionsMixin

from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin


class ProcessViewSet(ResolweCreateModelMixin,
                     mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     ResolweProcessPermissionsMixin,
                     ResolweCheckSlugMixin,
                     viewsets.GenericViewSet):
    """API view for :class:`Process` objects."""

    queryset = Process.objects.all().prefetch_related('contributor')
    serializer_class = ProcessSerializer
    permission_classes = (permissions_cls,)
    filter_class = ProcessFilter
    ordering_fields = ('id', 'created', 'modified', 'name', 'version')
    ordering = ('id',)
