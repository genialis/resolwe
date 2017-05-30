"""Entity viewset."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db.models import Max
from django.db.models.query import Prefetch

from rest_framework import exceptions, status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from resolwe.flow.filters import EntityFilter
from resolwe.flow.models import Collection, Data, Entity
from resolwe.flow.serializers import EntitySerializer
from resolwe.permissions.utils import remove_permission, update_permission

from .collection import CollectionViewSet


class EntityViewSet(CollectionViewSet):
    """API view for entities."""

    filter_class = EntityFilter
    serializer_class = EntitySerializer

    queryset = Entity.objects.prefetch_related(
        Prefetch('data', queryset=Data.objects.all().order_by('id')),
        'descriptor_schema',
        'contributor'
    ).annotate(
        latest_date=Max('data__modified')
    ).order_by('-latest_date')

    def _check_collection_permissions(self, collection_id, user):
        """Check that collection exists and user has `add` permission."""
        collection_query = Collection.objects.filter(pk=collection_id)
        if not collection_query.exists():
            raise exceptions.ValidationError('Collection id does not exist')

        collection = collection_query.first()
        if not user.has_perm('add_collection', obj=collection):
            if user.is_authenticated():
                raise exceptions.PermissionDenied()
            else:
                raise exceptions.NotFound()

    def set_content_permissions(self, user, obj, payload):
        """Apply permissions to data objects in ``Entity``."""
        # Data doesn't have "ADD" permission, so it has to be removed
        payload = remove_permission(payload, 'add')

        for data in obj.data.all():
            if user.has_perm('share_data', data):
                update_permission(data, payload)

    @detail_route(methods=[u'post'])
    def add_to_collection(self, request, pk=None):
        """Add Entity to a collection."""
        entity = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids` parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for collection_id in request.data['ids']:
            self._check_collection_permissions(collection_id, request.user)

        for collection_id in request.data['ids']:
            entity.collections.add(collection_id)

            collection = Collection.objects.get(pk=collection_id)
            for data in entity.data.all():
                collection.data.add(data)

        return Response()

    @detail_route(methods=[u'post'])
    def remove_from_collection(self, request, pk=None):
        """Remove Entity from a collection."""
        entity = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids` parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for collection_id in request.data['ids']:
            self._check_collection_permissions(collection_id, request.user)

        for collection_id in request.data['ids']:
            entity.collections.remove(collection_id)

            collection = Collection.objects.get(pk=collection_id)
            for data in entity.data.all():
                collection.data.remove(data)

        return Response()

    @detail_route(methods=[u'post'])
    def add_data(self, request, pk=None):
        """Add data to Entity and it's collection."""
        # add data to entity
        resp = super(EntityViewSet, self).add_data(request, pk)

        # add data to collections in which entity is
        entity = self.get_object()
        for collection in entity.collections.all():
            collection.data.add(*request.data['ids'])

        return resp

    @detail_route(methods=[u'post'])
    def remove_data(self, request, pk=None):
        """Remove Data from Entity and delete it if it is empty."""
        resp = super(EntityViewSet, self).remove_data(request, pk)

        entity = self.get_object()
        if entity.data.count() == 0:
            entity.delete()

        return resp
