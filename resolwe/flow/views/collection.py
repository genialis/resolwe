"""Collection viewset."""
from django.db import transaction
from django.db.models import Prefetch

from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import CollectionFilter
from resolwe.flow.models import Collection, DescriptorSchema
from resolwe.flow.models.annotations import AnnotationValue
from resolwe.flow.serializers import CollectionSerializer
from resolwe.flow.serializers.annotations import AnnotationFieldDictSerializer
from resolwe.observers.mixins import ObservableMixin
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import Permission, PermissionModel

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
    qs_permission_model = PermissionModel.objects.select_related("user", "group")

    queryset = Collection.objects.select_related("contributor").prefetch_related(
        "data", Prefetch("descriptor_schema", queryset=qs_descriptor_schema)
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
        """Prefetch permissions for current user."""
        return self.prefetch_current_user_permissions(self.queryset)

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
        queryset = Collection.objects.filter(id__in=ids).filter_for_user(
            request.user, Permission.VIEW
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


class CollectionViewSet(ObservableMixin, BaseCollectionViewSet):
    """API view for :class:`Collection` objects."""

    serializer_class = CollectionSerializer

    def get_queryset(self):
        """Prefetch entity data and return queryset."""
        return super().get_queryset().prefetch_related("entity_set")

    @action(detail=True, methods=["post"])
    def add_fields_to_collection(self, request, pk=None):
        """Add the given list of AnnotaitonFields to the given collection."""
        # No need to check for permissions, since post requires edit by default.
        collection = self.get_object()
        # Read and validate the request data.
        serializer = AnnotationFieldDictSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        # Add the fields to the collection.
        collection.annotation_fields.add(
            *serializer.validated_data["annotation_fields"]
        )
        return Response()

    @action(detail=True, methods=["post"])
    def remove_fields_from_collection(self, request, pk=None):
        """Remove the given list of AnnotaitonFields from the given collection.

        The required argument is annotation_fields and optional confirm_action.
        """
        # No need to check for permissions, since post requires edit by default.
        collection = self.get_object()
        serializer = AnnotationFieldDictSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        if not serializer.validated_data["confirm_action"]:
            raise exceptions.ValidationError(
                "Annotations for the given fields will be removed from the samples in "
                "the collection. Set 'confirm_action' argument to 'True' to confirm."
            )

        annotation_fields = serializer.validated_data["annotation_fields"]
        # Delete annotation values from the samples in this collection and remove the
        # annotation fields from the collection.
        with transaction.atomic():
            AnnotationValue.objects.filter(
                entity__collection=collection, field__in=annotation_fields
            ).delete()
            collection.annotation_fields.remove(*annotation_fields)
        return Response()
