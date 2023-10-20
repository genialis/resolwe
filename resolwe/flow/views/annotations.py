"""Annotations viewset."""


from rest_framework import exceptions, mixins, permissions, request, viewsets

from resolwe.flow.filters import (
    AnnotationFieldFilter,
    AnnotationPresetFilter,
    AnnotationValueFilter,
)
from resolwe.flow.models import AnnotationPreset, Entity
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
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    ResolweUpdateModelMixin,
    mixins.ListModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet,
):
    """Annotation value viewset."""

    # Users can only see the annotation values on the entities they have permission to
    # access. The actual permissions are checked in the AnnotationValueFilter. The
    # filter assures at least one of the entity filters is applied and the permissions
    # filters are applied inside filter_permissions method defined in
    # AnnotationValueMetaclass.
    # This behaviour is tested in test_list_filter_values.
    permission_classes = (permissions.AllowAny,)
    serializer_class = AnnotationValueSerializer
    filterset_class = AnnotationValueFilter
    queryset = AnnotationValue.objects.all()

    def _has_permissions_on_entity(self, entity: Entity) -> bool:
        """Has the authenticated user EDIT permission on the associated entity."""
        return (
            Entity.objects.filter(pk=entity.pk)
            .filter_for_user(self.request.user, Permission.EDIT)
            .exists()
        )

    def _get_entity(self, request: request.Request) -> AnnotationValue:
        """Get annotation value from request.

        :raises ValidationError: if the annotation value is not valid.
        :raises NotFound: if the user is not authenticated.
        """
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        serializer = self.get_serializer(data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        return serializer.validated_data["entity"]

    def create(self, request, *args, **kwargs):
        """Create annotation value.

        Authenticated users with edit permissions on the entity can create annotations.
        """
        if self._has_permissions_on_entity(self._get_entity(request)):
            return super().create(request, *args, **kwargs)
        raise exceptions.NotFound()

    def update(self, request, *args, **kwargs):
        """Update annotation values.

        Authenticated users with edit permission on the entity can update annotations.
        """
        entity = AnnotationValue.objects.get(pk=kwargs["pk"]).entity
        if self._has_permissions_on_entity(entity):
            return super().update(request, *args, **kwargs)
        raise exceptions.NotFound()

    def destroy(self, request, *args, **kwargs):
        """Destroy the annotation value."""
        entity = AnnotationValue.objects.get(pk=kwargs["pk"]).entity
        if self._has_permissions_on_entity(entity):
            return super().destroy(request, *args, **kwargs)
        raise exceptions.PermissionDenied()
