"""Annotations viewset."""


from typing import Any

from rest_framework import (
    exceptions,
    generics,
    mixins,
    permissions,
    response,
    status,
    viewsets,
)
from rest_framework.serializers import BaseSerializer

from resolwe.flow.filters import (
    AnnotationFieldFilter,
    AnnotationPresetFilter,
    AnnotationValueFilter,
)
from resolwe.flow.models import AnnotationPreset
from resolwe.flow.models.annotations import AnnotationField, AnnotationValue
from resolwe.flow.serializers.annotations import (
    AnnotationFieldSerializer,
    AnnotationPresetSerializer,
    AnnotationValueSerializer,
)
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import Permission

from .mixins import ResolweCreateModelMixin, ResolweUpdateModelMixin


class AnnotationPresetViewSet(
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.ListModelMixin,
    ResolwePermissionsMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`DescriptorSchema` objects."""

    serializer_class = AnnotationPresetSerializer
    permission_classes = (get_permissions_class(),)
    filterset_class = AnnotationPresetFilter

    # No need to specify default ordering: it is specified on the model.
    ordering_fields = ("id", "name", "sort_order")
    queryset = AnnotationPreset.objects.all()

    def get_queryset(self):
        """Get the presets for the current user."""
        return super().get_queryset().filter_for_user(self.request.user)


class AnnotationFieldViewSet(
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    """Annotation fields viewset."""

    permission_classes = (permissions.AllowAny,)
    serializer_class = AnnotationFieldSerializer
    filterset_class = AnnotationFieldFilter
    # No need to specify default ordering: it is specified on the model.
    ordering_fields = ("id", "name", "label", "group__sort_order", "sort_order")
    queryset = AnnotationField.objects.all()


class AnnotationValueViewSet(
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.CreateModelMixin,
    mixins.ListModelMixin,
    mixins.DestroyModelMixin,
    generics.UpdateAPIView,
    viewsets.GenericViewSet,
):
    """Annotation value viewset."""

    serializer_class = AnnotationValueSerializer
    filterset_class = AnnotationValueFilter
    queryset = AnnotationValue.objects.all()
    permission_classes = (get_permissions_class(),)

    def get_serializer(self, *args: Any, **kwargs: Any) -> BaseSerializer:
        """Get serializer instance depending on the request type."""
        kwargs_many = kwargs.get("many", False)
        kwargs["many"] = isinstance(self.request.data, list) or kwargs_many
        return super().get_serializer(*args, **kwargs)

    def _check_permissions(self, serializer):
        """Check if user has edit permission on entities."""
        validated_data = (
            serializer.validated_data
            if isinstance(serializer.validated_data, list)
            else [serializer.validated_data]
        )
        # Check permissions on entities.
        if not all(
            entity.has_permission(Permission.EDIT, self.request.user)
            for entity in {value["entity"] for value in validated_data}
        ):
            raise exceptions.PermissionDenied()

    def perform_create(self, serializer: BaseSerializer) -> None:
        """Perform create annotation value(s).

        The permission on entities must be checked.
        """
        self._check_permissions(serializer)
        return super().perform_create(serializer)

    def update(self, request, *args, pk=None, **kwargs):
        """Update annotation value(s).

        When posting multiple values, the request is treated as a bulk update. The bulk
        update can create, update or delete values. Values are deleted when the value
        is set no None.
        """
        # Regular update on a detail view.
        if pk is not None:
            return super().update(request, *args, pk=pk, **kwargs)

        # Bulk update / create / delete.
        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        self._check_permissions(serializer)
        serializer.update(None, serializer.validated_data)
        headers = self.get_success_headers(serializer.data)
        return response.Response(
            serializer.data, status=status.HTTP_200_OK, headers=headers
        )
