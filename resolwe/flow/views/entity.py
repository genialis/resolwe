"""Entity viewset."""

from typing import Optional

from django.db.models import F, Func, OuterRef, Prefetch, Subquery
from django.db.models.functions import Coalesce
from drf_spectacular.utils import extend_schema
from rest_framework import exceptions, serializers, status
from rest_framework.decorators import action
from rest_framework.request import Request
from rest_framework.response import Response

from resolwe.flow.filters import EntityFilter
from resolwe.flow.models import Data, Entity
from resolwe.flow.serializers import EntitySerializer
from resolwe.flow.serializers.annotations import AnnotationsByPathSerializer
from resolwe.observers.mixins import ObservableMixin
from resolwe.observers.views import BackgroundTaskSerializer

from .collection import BaseCollectionViewSet
from .utils import get_collection_for_user


class MoveEntityToCollectionSerializer(serializers.Serializer):
    """Serializer for moving entities to a collection."""

    ids = serializers.ListField(child=serializers.IntegerField())
    destination_collection = serializers.IntegerField()


class EntityViewSet(ObservableMixin, BaseCollectionViewSet):
    """API view for entities."""

    serializer_class = EntitySerializer
    filterset_class = EntityFilter

    data_count_subquery = (
        Data.objects.filter(entity_id=OuterRef("pk"))
        .annotate(count=Func(F("id"), function="Count"))
        .values("count")
    )
    data_status_subquery = (
        Data.objects.filter(entity_id=OuterRef("pk"))
        .annotate(
            count=Coalesce(
                Func(
                    F("status"),
                    function="ARRAY_AGG",
                    template="%(function)s(DISTINCT %(expressions)s)",
                ),
                [],
            )
        )
        .values("count")
    )
    queryset = (
        Entity.objects.select_related("contributor")
        .prefetch_related(
            Prefetch("collection", queryset=BaseCollectionViewSet.queryset)
        )
        .annotate(data_statuses=Subquery(data_status_subquery))
        .annotate(data_count=Subquery(data_count_subquery))
        .annotate_status()
    )

    def _get_entities(self, user, ids):
        """Return entities queryset based on provided entity ids."""
        queryset = Entity.objects.filter(id__in=ids).filter_for_user(user)
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        return queryset

    @extend_schema(
        request=MoveEntityToCollectionSerializer(), responses={status.HTTP_200_OK: None}
    )
    @action(detail=False, methods=["post"])
    def move_to_collection(self, request, *args, **kwargs):
        """Move samples from source to destination collection."""

        serializer = MoveEntityToCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        ids = serializer.validated_data["ids"]
        dst_collection_id = serializer.validated_data["destination_collection"]
        dst_collection = get_collection_for_user(dst_collection_id, request.user)
        entity_qs = self._get_entities(request.user, ids)
        task = entity_qs.move_to_collection(dst_collection, request.user)
        return Response(
            status=status.HTTP_200_OK,
            data=BackgroundTaskSerializer(task).data,
        )

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

    @extend_schema(
        request=AnnotationsByPathSerializer(many=True),
        responses={status.HTTP_200_OK: None},
    )
    @action(detail=True, methods=["post"])
    def set_annotations(self, request: Request, pk: Optional[int] = None):
        """Add the given list of AnnotationFields to the given entity."""
        # No need to check for permissions, since post requires edit by default.
        entity = self.get_object()
        # Read and validate the request data.
        serializer = AnnotationsByPathSerializer(data=request.data, many=True)
        serializer.is_valid(raise_exception=True)
        annotations = {value["field_path"]: value["value"] for value in serializer.data}
        entity.update_annotations(annotations, request.user)
        return Response()
