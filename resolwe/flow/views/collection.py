"""Collection viewset."""
from django.db.models import Count, Prefetch, Q

from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import CollectionFilter
from resolwe.flow.models import Collection, Data, DescriptorSchema
from resolwe.flow.serializers import CollectionSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import update_permission

from .mixins import (
    ParametersMixin,
    ResolweCheckSlugMixin,
    ResolweCreateModelMixin,
    ResolweUpdateModelMixin,
)


class BaseCollectionViewSet(
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.DestroyModelMixin,
    mixins.ListModelMixin,
    ResolwePermissionsMixin,
    ResolweCheckSlugMixin,
    ParametersMixin,
    viewsets.GenericViewSet,
):
    """Base API view for :class:`Collection` objects."""

    qs_descriptor_schema = DescriptorSchema.objects.select_related("contributor")

    queryset = Collection.objects.select_related("contributor").prefetch_related(
        Prefetch("descriptor_schema", queryset=qs_descriptor_schema),
    )

    filter_class = CollectionFilter
    permission_classes = (get_permissions_class(),)

    ordering_fields = (
        "contributor",
        "contributor__first_name",
        "contributor__last_name",
        "created",
        "id",
        "modified",
        "name",
    )
    ordering = "id"

    def get_queryset(self):
        """Annotate Get requests with data counts and return queryset."""
        if self.request.method == "GET":
            return self.queryset.annotate(
                data_count=Count("data", distinct=True),
                data_uploading_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_UPLOADING)
                ),
                data_resolving_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_RESOLVING)
                ),
                data_waiting_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_WAITING)
                ),
                data_preparing_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_PREPARING)
                ),
                data_processing_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_PROCESSING)
                ),
                data_done_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_DONE)
                ),
                data_error_count=Count(
                    "data", distinct=True, filter=Q(data__status=Data.STATUS_ERROR)
                ),
            )

        return self.queryset

    def set_content_permissions(self, user, obj, payload):
        """Apply permissions to data objects and entities in ``Collection``."""
        for entity in obj.entity_set.all():
            if user.has_perm("share_entity", entity):
                update_permission(entity, payload)

        for data in obj.data.all():
            if user.has_perm("share_data", data):
                update_permission(data, payload)

    def create(self, request, *args, **kwargs):
        """Only authenticated users can create new collections."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)

    @action(detail=False, methods=["post"])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Collection`` models."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        ids = self.get_ids(request.data)
        queryset = get_objects_for_user(
            request.user, "view_collection", Collection.objects.filter(id__in=ids)
        )
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Collections with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        duplicated = queryset.duplicate(contributor=request.user)

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)


class CollectionViewSet(BaseCollectionViewSet):
    """API view for :class:`Collection` objects."""

    serializer_class = CollectionSerializer

    def get_queryset(self):
        """Annotate Get requests with entity count and return queryset."""
        queryset = super().get_queryset()

        if self.request.method == "GET":
            return queryset.annotate(entity_count=Count("entity", distinct=True))

        return queryset
