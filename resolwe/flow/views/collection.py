"""Collection viewset."""

from django.db import transaction
from django.db.models import F, Func, OuterRef, Prefetch, Subquery
from django.db.models.functions import Coalesce
from drf_spectacular.utils import extend_schema
from rest_framework import exceptions, mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import CollectionFilter
from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity
from resolwe.flow.serializers import CollectionSerializer
from resolwe.flow.serializers.annotations import AnnotationFieldDictSerializer
from resolwe.observers.mixins import ObservableMixin
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import PermissionModel

from .mixins import (
    ResolweBackgroundDeleteMixin,
    ResolweBackgroundDuplicateMixin,
    ResolweCheckSlugMixin,
    ResolweCreateModelMixin,
    ResolweUpdateModelMixin,
)


class BaseCollectionViewSet(
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    ResolweBackgroundDeleteMixin,
    mixins.ListModelMixin,
    ResolwePermissionsMixin,
    ResolweCheckSlugMixin,
    ResolweBackgroundDuplicateMixin,
    viewsets.GenericViewSet,
):
    """Base API view for :class:`Collection` objects."""

    qs_permission_model = PermissionModel.objects.select_related("user", "group")

    # Add entity count as a subquery: by default Django joins tables and this explodes
    # when one tries to annotate the collection query with data and entity count at the
    # same time.
    entity_count_subquery = (
        Entity.objects.filter(collection_id=OuterRef("pk"))
        .annotate(count=Func(F("id"), function="Count"))
        .values("count")
    )
    data_count_subquery = (
        Data.objects.filter(collection_id=OuterRef("pk"))
        .annotate(count=Func(F("id"), function="Count"))
        .values("count")
    )
    data_status_subquery = (
        Data.objects.filter(collection_id=OuterRef("pk"))
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

    qs_descriptor_schema = DescriptorSchema.all_objects.select_related("contributor")

    queryset = (
        Collection.objects.select_related("contributor")
        .prefetch_related(Prefetch("descriptor_schema", queryset=qs_descriptor_schema))
        .annotate(data_statuses=Subquery(data_status_subquery))
        .annotate(data_count=Subquery(data_count_subquery))
        .annotate(entity_count=Subquery(entity_count_subquery))
    )

    filterset_class = CollectionFilter
    permission_classes = (get_permissions_class(),)

    ordering_fields = (
        "contributor",
        "contributor__first_name",
        "contributor__last_name",
        "collection__name",
        "created",
        "id",
        "modified",
        "name",
    )
    ordering = "id"

    def get_queryset(self):
        """Prefetch permissions for current user."""
        return self.prefetch_current_user_permissions(self.queryset)

    def create(self, request, *args, **kwargs):
        """Only authenticated users can create new collections."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)


class CollectionViewSet(ObservableMixin, BaseCollectionViewSet):
    """API view for :class:`Collection` objects."""

    serializer_class = CollectionSerializer

    @extend_schema(
        request=AnnotationFieldDictSerializer(),
        responses={status.HTTP_204_NO_CONTENT: None},
    )
    @action(detail=True, methods=["post"])
    @transaction.atomic
    def set_annotation_fields(self, request, pk=None):
        """Set AnnotationFields on collection."""
        # No need to check for permissions, since post requires edit by default.
        collection = self.get_object()
        # Read and validate the request data.
        serializer = AnnotationFieldDictSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        # Set the new annotation fields.
        collection.annotation_fields.set(serializer.validated_data["annotation_fields"])
        return Response(status=status.HTTP_204_NO_CONTENT)
