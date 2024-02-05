"""Descriptor schema viewset."""

from rest_framework import mixins, viewsets

from resolwe.flow.filters import DescriptorSchemaFilter
from resolwe.flow.models import DescriptorSchema
from resolwe.flow.serializers import DescriptorSchemaSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import PermissionModel


class DescriptorSchemaViewSet(
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    ResolwePermissionsMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`DescriptorSchema` objects."""

    qs_permission_model = PermissionModel.objects.select_related("user", "group")
    queryset = DescriptorSchema.objects.all().select_related("contributor")
    serializer_class = DescriptorSchemaSerializer
    permission_classes = (get_permissions_class(),)
    filterset_class = DescriptorSchemaFilter
    ordering_fields = ("id", "created", "modified", "name", "version")
    ordering = ("id",)

    def get_queryset(self):
        """Prefetch permissions for current user."""
        return self.prefetch_current_user_permissions(self.queryset)
