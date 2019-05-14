"""Entity viewset."""
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

from django.db.models import Max
from django.db.models.query import Prefetch

from rest_framework import exceptions, status
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.models import Collection, Data, Entity
from resolwe.flow.serializers import EntitySerializer
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import remove_permission, update_permission

from ..elastic_indexes import EntityDocument
from .collection import CollectionViewSet


class EntityViewSet(CollectionViewSet):
    """API view for entities."""

    serializer_class = EntitySerializer
    document_class = EntityDocument

    queryset = Entity.objects.prefetch_related(
        Prefetch('data', queryset=Data.objects.all().order_by('id')),
        'descriptor_schema',
        'contributor'
    ).annotate(
        latest_date=Max('data__modified')
    ).order_by('-latest_date')

    filtering_fields = CollectionViewSet.filtering_fields + (
        'descriptor_completed', 'collections', 'type'
    )

    def get_queryset(self):  # pylint: disable=method-hidden
        """Return queryset."""
        if self.request and self.request.query_params.get('hydrate_data', False):
            return self.queryset.prefetch_related('data__entity_set', 'data__collection_set')

        return self.queryset

    def _get_collection_for_user(self, collection_id, user):
        """Check that collection exists and user has `add` permission."""
        collection_query = Collection.objects.filter(pk=collection_id)
        if not collection_query.exists():
            raise exceptions.ValidationError('Collection id does not exist')

        collection = collection_query.first()
        if not user.has_perm('add_collection', obj=collection):
            if user.is_authenticated:
                raise exceptions.PermissionDenied()
            else:
                raise exceptions.NotFound()

        return collection

    def _get_entities(self, user, ids):
        """Return entities queryset based on provided entity ids."""
        queryset = get_objects_for_user(user, 'view_entity', Entity.objects.filter(id__in=ids))
        actual_ids = queryset.values_list('id', flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}" .format(', '.join(map(str, missing_ids)))
            )

        return queryset

    def set_content_permissions(self, user, obj, payload):
        """Apply permissions to data objects in ``Entity``."""
        # Data doesn't have "ADD" permission, so it has to be removed
        payload = remove_permission(payload, 'add')

        for data in obj.data.all():
            if user.has_perm('share_data', data):
                update_permission(data, payload)

    def destroy(self, request, *args, **kwargs):
        """Destroy a model instance.

        If ``delete_content`` flag is set in query parameters, also all
        Data objects contained in entity will be deleted.
        """
        obj = self.get_object()
        user = request.user

        if strtobool(request.query_params.get('delete_content', 'false')):
            for data in obj.data.all():
                if user.has_perm('edit_data', data):
                    data.delete()

            # If all data objects in an entity are removed, the entity may
            # have already been removed, so there is no need to call destroy.
            if not Entity.objects.filter(pk=obj.pk).exists():
                return Response(status=status.HTTP_204_NO_CONTENT)

        # NOTE: Collection's ``destroy`` method should be skiped, so we
        # intentionaly call it's parent.
        return super(CollectionViewSet, self).destroy(  # pylint: disable=no-member,bad-super-call
            request, *args, **kwargs
        )

    @action(detail=True, methods=['post'])
    def add_to_collection(self, request, pk=None):
        """Add Entity to a collection."""
        entity = self.get_object()

        # TODO use `self.get_ids` (and elsewhere). Backwards
        # incompatible because raised error's response contains
        # ``detail`` instead of ``error``).
        if 'ids' not in request.data:
            return Response({"error": "`ids` parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for collection_id in request.data['ids']:
            self._get_collection_for_user(collection_id, request.user)

        for collection_id in request.data['ids']:
            entity.collections.add(collection_id)

            collection = Collection.objects.get(pk=collection_id)
            for data in entity.data.all():
                collection.data.add(data)

        return Response()

    @action(detail=True, methods=['post'])
    def remove_from_collection(self, request, pk=None):
        """Remove Entity from a collection."""
        entity = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids` parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for collection_id in request.data['ids']:
            self._get_collection_for_user(collection_id, request.user)

        for collection_id in request.data['ids']:
            entity.collections.remove(collection_id)

            collection = Collection.objects.get(pk=collection_id)
            for data in entity.data.all():
                collection.data.remove(data)

        return Response()

    @action(detail=True, methods=['post'])
    def add_data(self, request, pk=None):
        """Add data to Entity and it's collection."""
        # add data to entity
        resp = super().add_data(request, pk)

        # add data to collections in which entity is
        entity = self.get_object()
        for collection in entity.collections.all():
            collection.data.add(*request.data['ids'])

        return resp

    @action(detail=False, methods=['post'])
    def move_to_collection(self, request, *args, **kwargs):
        """Move samples from source to destination collection."""
        ids = self.get_ids(request.data)
        src_collection_id = self.get_id(request.data, 'source_collection')
        dst_collection_id = self.get_id(request.data, 'destination_collection')

        src_collection = self._get_collection_for_user(src_collection_id, request.user)
        dst_collection = self._get_collection_for_user(dst_collection_id, request.user)

        entity_qs = self._get_entities(request.user, ids)
        entity_qs.move_to_collection(src_collection, dst_collection)

        return Response()

    # NOTE: This can be deleted when DRF will support select_for_update
    #       on updates and ResolweUpdateModelMixin will use it.
    #       https://github.com/encode/django-rest-framework/issues/4675
    def update(self, request, *args, **kwargs):
        """Update an entity.

        Original queryset produces a temporary database table whose rows
        cannot be selected for an update. As a workaround, we patch
        get_queryset function to return only Entity objects without
        additional data that is not needed for the update.
        """
        orig_get_queryset = self.get_queryset

        def patched_get_queryset():
            """Patched get_queryset method."""
            entity_ids = orig_get_queryset().values_list('id', flat=True)
            return Entity.objects.filter(id__in=entity_ids)

        self.get_queryset = patched_get_queryset
        resp = super().update(request, *args, **kwargs)
        self.get_queryset = orig_get_queryset
        return resp

    @action(detail=False, methods=['post'])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Entity`` models."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        inherit_collections = request.data.get('inherit_collections', False)
        ids = self.get_ids(request.data)
        queryset = get_objects_for_user(request.user, 'view_entity', Entity.objects.filter(id__in=ids))
        actual_ids = queryset.values_list('id', flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(', '.join(map(str, missing_ids)))
            )

        duplicated = queryset.duplicate(contributor=request.user, inherit_collections=inherit_collections)

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)
