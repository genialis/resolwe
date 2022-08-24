"""Annotations viewset."""


from rest_framework import mixins, permissions, viewsets

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

    permission_classes = (permissions.IsAuthenticated,)
    serializer_class = AnnotationFieldSerializer
    filterset_class = AnnotationFieldFilter
    # No need to specify default ordering: it is specified on the model.
    ordering_fields = ("id", "name", "label", "group__sort_order", "sort_order")
    queryset = AnnotationField.objects.all()


class AnnotationValueViewSet(
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    """Annotation value viewset."""

    permission_classes = (permissions.IsAuthenticated,)
    serializer_class = AnnotationValueSerializer
    filterset_class = AnnotationValueFilter
    queryset = AnnotationValue.objects.all()
