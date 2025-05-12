"""Collection viewset."""

from typing import Optional

from django.db import transaction
from django.db.models import F, Func, OuterRef, Prefetch, QuerySet, Subquery
from django.db.models.functions import Coalesce
from drf_spectacular.utils import extend_schema
from rest_framework import exceptions, mixins, request, serializers, status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import SAFE_METHODS
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
        "status",
    )
    ordering = "id"

    def get_queryset(self) -> QuerySet:
        """Prefetch permissions for current user."""
        # Only annotate the queryset with status on safe methods. When updating
        # the annotation interfers with update (as it adds group by statement).
        if self.request.method in SAFE_METHODS:
            self.queryset = self.queryset.annotate_status()
        return self.prefetch_current_user_permissions(self.queryset)

    def create(self, request: request.Request, *args, **kwargs) -> Response:
        """Only authenticated users can create new collections."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)


class CollectionViewSet(ObservableMixin, BaseCollectionViewSet):
    """API view for :class:`Collection` objects."""

    serializer_class = CollectionSerializer

    def _set_fields(self, request: request.Request, property_name: str) -> Response:
        """Set the fields on the collection."""
        collection = self.get_object()
        # Validate that the collection has the specified property with a callable 'set' method.
        if not hasattr(collection, property_name):
            raise serializers.ValidationError(
                {
                    property_name: f"The collection does not have a '{property_name}' attribute."
                }
            )
        attribute = getattr(collection, property_name)
        if not callable(getattr(attribute, "set", None)):
            raise serializers.ValidationError(
                {
                    property_name: f"The '{property_name}' attribute does not have a callable 'set' method."
                }
            )
        # Read and validate the request data. Get the serializer from the DRF viewset
        # since the get_serializer method is overridden in the collection viewset class.
        SerializerClass = viewsets.GenericViewSet.get_serializer_class(self)
        serializer = SerializerClass(data=request.data)
        # Validate the serializer data.
        serializer.is_valid(raise_exception=True)
        getattr(collection, property_name).set(serializer.validated_data[property_name])
        return Response(status=status.HTTP_204_NO_CONTENT)

    @extend_schema(
        request=AnnotationFieldDictSerializer(),
        responses={status.HTTP_204_NO_CONTENT: None},
    )
    @action(
        detail=True, methods=["post"], serializer_class=AnnotationFieldDictSerializer
    )
    @transaction.atomic
    def set_annotation_fields(
        self, request: request.Request, pk: Optional[int] = None
    ) -> Response:
        """Set AnnotationFields on collection."""
        return self._set_fields(request, "annotation_fields")
