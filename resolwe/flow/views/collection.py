"""Collection viewset."""
from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import CollectionFilter
from resolwe.flow.models import Collection
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


class CollectionViewSet(
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
    """API view for :class:`Collection` objects."""

    queryset = Collection.objects.all().prefetch_related(
        "descriptor_schema", "contributor"
    )
    serializer_class = CollectionSerializer
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
