"""Data viewset."""
from django.db.models import Prefetch

from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import DataFilter
from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.models.utils import fill_with_defaults
from resolwe.flow.serializers import DataSerializer
from resolwe.flow.utils import get_data_checksum
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
from .utils import get_collection_for_user


class DataViewSet(
    ObservableMixin,
    ResolweCreateModelMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.DestroyModelMixin,
    ResolwePermissionsMixin,
    ResolweCheckSlugMixin,
    ParametersMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`Data` objects."""

    qs_permission_model = PermissionModel.objects.select_related("user", "group")
    qs_collection_ds = DescriptorSchema.objects.select_related("contributor")
    qs_collection = Collection.objects.select_related("contributor")
    qs_collection = qs_collection.prefetch_related(
        "data",
        "entity_set",
        Prefetch("descriptor_schema", queryset=qs_collection_ds),
    )

    qs_descriptor_schema = DescriptorSchema.objects.select_related("contributor")

    qs_entity_col_ds = DescriptorSchema.objects.select_related("contributor")
    qs_entity_col = Collection.objects.select_related("contributor")
    qs_entity_col = qs_entity_col.prefetch_related(
        "data",
        "entity_set",
        Prefetch("descriptor_schema", queryset=qs_entity_col_ds),
    )
    qs_entity_ds = DescriptorSchema.objects.select_related("contributor")
    qs_entity = Entity.objects.select_related("contributor")
    qs_entity = qs_entity.prefetch_related(
        "data",
        Prefetch("collection", queryset=qs_entity_col),
        Prefetch("descriptor_schema", queryset=qs_entity_ds),
    )
    qs_process = Process.objects.select_related("contributor")
    queryset = Data.objects.select_related("contributor").prefetch_related(
        Prefetch("collection", queryset=qs_collection),
        Prefetch("descriptor_schema", queryset=qs_descriptor_schema),
        Prefetch("entity", queryset=qs_entity),
        Prefetch("process", queryset=qs_process),
    )

    serializer_class = DataSerializer
    filter_class = DataFilter
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
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Data`` objects."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        inherit_collection = request.data.get("inherit_collection", False)
        ids = self.get_ids(request.data)
        queryset = Data.objects.filter(id__in=ids).filter_for_user(
            request.user, Permission.VIEW
        )
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Data objects with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        duplicated = queryset.duplicate(
            contributor=request.user,
            inherit_collection=inherit_collection,
        )

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)

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

    @action(detail=True)
    def parents(self, request, pk=None):
        """Return parents of the current data object."""
        return self._parents_children(request, self.get_object().parents)

    @action(detail=True)
    def children(self, request, pk=None):
        """Return children of the current data object."""
        return self._parents_children(request, self.get_object().children.all())

    @action(detail=False, methods=["post"])
    def move_to_collection(self, request, *args, **kwargs):
        """Move data objects to destination collection."""
        ids = self.get_ids(request.data)
        dst_collection_id = self.get_id(request.data, "destination_collection")

        dst_collection = get_collection_for_user(dst_collection_id, request.user)

        queryset = self._get_data(request.user, ids)
        queryset.move_to_collection(dst_collection)

        return Response()

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
