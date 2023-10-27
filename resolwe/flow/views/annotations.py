"""Annotations viewset."""


from rest_framework import exceptions, mixins, permissions, request, viewsets

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
    ResolweCreateModelMixin,
    mixins.ListModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    """Annotation value viewset."""

    serializer_class = AnnotationValueSerializer
    filterset_class = AnnotationValueFilter
    queryset = AnnotationValue.objects.all()
    permission_classes = (get_permissions_class(),)

    def _get_entity(self, request: request.Request) -> AnnotationValue:
        """Get annotation value from request.

        :raises ValidationError: if the annotation value is not valid.
        :raises NotFound: if the user is not authenticated.
        """
        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        return serializer.validated_data["entity"]

    def create(self, request, *args, **kwargs):
        """Create annotation value.

        Authenticated users with edit permissions on the entity can create annotations.
        """
        entity = self._get_entity(request)
        if entity.has_permission(Permission.EDIT, request.user):
            return super().create(request, *args, **kwargs)
        elif entity.has_permission(Permission.VIEW, request.user):
            raise exceptions.PermissionDenied()
        else:
            raise exceptions.NotFound()
