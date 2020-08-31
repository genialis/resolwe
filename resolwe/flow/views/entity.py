"""Entity viewset."""
from django.db.models import Prefetch

from rest_framework import exceptions
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import EntityFilter
from resolwe.flow.models import Collection, DescriptorSchema, Entity
from resolwe.flow.serializers import EntitySerializer
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import update_permission

from .collection import BaseCollectionViewSet
from .utils import get_collection_for_user


class EntityViewSet(BaseCollectionViewSet):
    """API view for entities."""

    qs_collection_ds = DescriptorSchema.objects.select_related("contributor")
    qs_collection = Collection.objects.select_related("contributor")
    qs_collection = qs_collection.prefetch_related(
        "data",
        "entity_set",
        Prefetch("descriptor_schema", queryset=qs_collection_ds),
    )

    qs_descriptor_schema = DescriptorSchema.objects.select_related("contributor")

    queryset = Entity.objects.select_related("contributor").prefetch_related(
        "data",
        Prefetch("collection", queryset=qs_collection),
        Prefetch("descriptor_schema", queryset=qs_descriptor_schema),
    )
    serializer_class = EntitySerializer
    filter_class = EntityFilter

    def _get_entities(self, user, ids):
        """Return entities queryset based on provided entity ids."""
        queryset = get_objects_for_user(
            user, "view_entity", Entity.objects.filter(id__in=ids)
        )
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        return queryset

    def set_content_permissions(self, user, obj, payload):
        """Apply permissions to data objects in ``Entity``."""
        for data in obj.data.all():
            if user.has_perm("share_data", data):
                update_permission(data, payload)

    @action(detail=False, methods=["post"])
    def move_to_collection(self, request, *args, **kwargs):
        """Move samples from source to destination collection."""
        ids = self.get_ids(request.data)
        src_collection_id = self.get_id(request.data, "source_collection")
        dst_collection_id = self.get_id(request.data, "destination_collection")

        src_collection = get_collection_for_user(src_collection_id, request.user)
        dst_collection = get_collection_for_user(dst_collection_id, request.user)

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
            entity_ids = orig_get_queryset().values_list("id", flat=True)
            return Entity.objects.filter(id__in=entity_ids)

        self.get_queryset = patched_get_queryset
        resp = super().update(request, *args, **kwargs)
        self.get_queryset = orig_get_queryset
        return resp

    @action(detail=False, methods=["post"])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Entity`` models."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        inherit_collection = request.data.get("inherit_collection", False)
        ids = self.get_ids(request.data)
        queryset = get_objects_for_user(
            request.user, "view_entity", Entity.objects.filter(id__in=ids)
        )
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        duplicated = queryset.duplicate(
            contributor=request.user, inherit_collection=inherit_collection
        )

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)
