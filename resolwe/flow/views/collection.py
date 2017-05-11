"""Collection viewset."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db.models.query import Prefetch

from rest_framework import mixins, status, viewsets
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from resolwe.flow.filters import CollectionFilter
from resolwe.flow.models import Collection, Data
from resolwe.flow.serializers import CollectionSerializer
from resolwe.permissions.loader import permissions_cls
from resolwe.permissions.mixins import ResolwePermissionsMixin

from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin, ResolweUpdateModelMixin


class CollectionViewSet(ResolweCreateModelMixin,
                        mixins.RetrieveModelMixin,
                        ResolweUpdateModelMixin,
                        mixins.DestroyModelMixin,
                        mixins.ListModelMixin,
                        ResolwePermissionsMixin,
                        ResolweCheckSlugMixin,
                        viewsets.GenericViewSet):
    """API view for :class:`Collection` objects."""

    queryset = Collection.objects.all().prefetch_related(
        'descriptor_schema',
        'contributor',
        Prefetch('data', queryset=Data.objects.all().order_by('id'))
    )
    serializer_class = CollectionSerializer
    permission_classes = (permissions_cls,)
    filter_class = CollectionFilter
    ordering_fields = ('id', 'created', 'modified', 'name')
    ordering = ('id',)

    @detail_route(methods=[u'post'])
    def add_data(self, request, pk=None):
        """Add data to collection."""
        collection = self.get_object()

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
        """Remove data from collection."""
        collection = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids`parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for data_id in request.data['ids']:
            collection.data.remove(data_id)

        return Response()
