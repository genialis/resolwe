"""Data viewset."""

from django.db.models import Prefetch
from drf_spectacular.utils import extend_schema
from rest_framework import (
    exceptions,
    mixins,
    permissions,
    serializers,
    status,
    viewsets,
)
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import DataFilter
from resolwe.flow.models import Data, DescriptorSchema, Process
from resolwe.flow.models.utils import fill_with_defaults
from resolwe.flow.serializers import DataSerializer
from resolwe.flow.utils import get_data_checksum
from resolwe.observers.mixins import ObservableMixin
from resolwe.observers.views import BackgroundTaskSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import Permission, PermissionModel

from .collection import BaseCollectionViewSet
from .entity import EntityViewSet
from .mixins import (
    ResolweBackgroundDeleteMixin,
    ResolweBackgroundDuplicateMixin,
    ResolweCheckSlugMixin,
    ResolweCreateModelMixin,
    ResolweUpdateModelMixin,
)
from .utils import get_collection_for_user


class IsStaffuser(permissions.BasePermission):
    """Allow access only to staff users."""

    message = "Only staff users are allowed."

    def has_permission(self, request, view):
        """Return true when request is allowed."""
        return bool(
            request.user and request.user.is_authenticated and request.user.is_staff
        )


class MoveDataToCollectionSerializer(serializers.Serializer):
    """Deserializer for data move to collection endpoint."""

    ids = serializers.ListField(child=serializers.IntegerField())
    destination_collection = serializers.IntegerField()


class RestartSerializer(serializers.Serializer):
    """Serializer for restarting a Data object."""

    resource_overrides = serializers.JSONField(required=False, default=dict)


class DataViewSet(
    ObservableMixin,
    ResolweCreateModelMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    ResolweBackgroundDeleteMixin,
    ResolwePermissionsMixin,
    ResolweCheckSlugMixin,
    ResolweBackgroundDuplicateMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`Data` objects."""

    qs_permission_model = PermissionModel.objects.select_related("user", "group")
    qs_descriptor_schema = DescriptorSchema.all_objects.select_related("contributor")
    qs_process = Process.all_objects.select_related("contributor")
    queryset = Data.objects.select_related("contributor").prefetch_related(
        Prefetch("collection", queryset=BaseCollectionViewSet.queryset),
        Prefetch("descriptor_schema", queryset=qs_descriptor_schema),
        Prefetch("entity", queryset=EntityViewSet.queryset),
        Prefetch("process", queryset=qs_process),
    )

    serializer_class = DataSerializer
    filterset_class = DataFilter
    permission_classes = (get_permissions_class(),)

    ordering_fields = (
        "contributor",
        "contributor__first_name",
        "contributor__last_name",
        "created",
        "finished",
        "id",
        "modified",
        "name",
        "process__name",
        "process__type",
        "started",
        "entity__name",
    )
    ordering = "-created"

    def get_queryset(self):
        """Prefetch permissions for current user."""
        return self.prefetch_current_user_permissions(self.queryset)

    @action(detail=False, methods=["post"])
    def get_or_create(self, request, *args, **kwargs):
        """Get ``Data`` object if similar already exists, otherwise create it."""
        response = self.perform_get_or_create(request, *args, **kwargs)
        if response:
            return response

        return super().create(request, *args, **kwargs)

    def perform_get_or_create(self, request, *args, **kwargs):
        """Perform "get_or_create" - return existing object if found."""
        self.define_contributor(request)
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        process = serializer.validated_data.get("process")
        process_input = request.data.get("input", {})

        fill_with_defaults(process_input, process.input_schema)

        checksum = get_data_checksum(process_input, process.slug, process.version)
        data_qs = Data.objects.filter(
            checksum=checksum,
            process__persistence__in=[
                Process.PERSISTENCE_CACHED,
                Process.PERSISTENCE_TEMP,
            ],
        )
        data_qs = data_qs.filter_for_user(request.user)
        if data_qs.exists():
            data = data_qs.order_by("created").last()
            serializer = self.get_serializer(data)
            return Response(serializer.data)

    def _parents_children(self, request, queryset):
        """Process given queryset and return serialized objects."""
        queryset = queryset.filter_for_user(request.user)
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    @extend_schema(
        filters=False, responses={status.HTTP_200_OK: DataSerializer(many=True)}
    )
    @action(detail=True)
    def parents(self, request, pk=None):
        """Return parents of the current data object."""
        return self._parents_children(request, self.get_object().parents)

    @extend_schema(
        filters=False,
        request=RestartSerializer(),
        responses={status.HTTP_200_OK: DataSerializer(many=True)},
    )
    @action(detail=True, methods=["post"], permission_classes=[IsStaffuser])
    def restart(self, request, pk=None):
        """Restart the current data object."""
        argument_validator = RestartSerializer(data=request.data)
        argument_validator.is_valid(raise_exception=True)
        data = self.get_object()
        try:
            data.restart(**argument_validator.validated_data)
        except RuntimeError as e:
            raise exceptions.ValidationError(str(e))
        return Response(self.get_serializer(data).data)

    @action(detail=True)
    def children(self, request, pk=None):
        """Return children of the current data object."""
        return self._parents_children(request, self.get_object().children.all())

    @extend_schema(
        request=MoveDataToCollectionSerializer(),
        responses={status.HTTP_200_OK: None},
    )
    @action(detail=False, methods=["post"])
    def move_to_collection(self, request, *args, **kwargs):
        """Move data objects to destination collection."""
        serializer = MoveDataToCollectionSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        ids = serializer.validated_data["ids"]
        dst_collection_id = serializer.validated_data["destination_collection"]

        dst_collection = get_collection_for_user(dst_collection_id, request.user)

        queryset = self._get_data(request.user, ids)
        task = queryset.move_to_collection(dst_collection, request.user)
        return Response(
            status=status.HTTP_200_OK,
            data=BackgroundTaskSerializer(task).data,
        )

    def _get_data(self, user, ids):
        """Return data objects queryset based on provided ids."""
        queryset = Data.objects.filter(id__in=ids).filter_for_user(user)
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Data objects with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        for data in queryset:
            collection = data.collection
            if collection and not user.has_perm(Permission.EDIT, obj=collection):
                if user.is_authenticated:
                    raise exceptions.PermissionDenied()
                else:
                    raise exceptions.NotFound()

        return queryset
