"""Descriptor schema viewset."""
from django.db.models import Prefetch, Q

from rest_framework import mixins, viewsets

from resolwe.flow.filters import DescriptorSchemaFilter
from resolwe.flow.models import DescriptorSchema
from resolwe.flow.serializers import DescriptorSchemaSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import PermissionModel
from resolwe.permissions.utils import get_anonymous_user, get_user


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
        """Get the queryset for the given request.

        Prefetch only permissions for the given user, not all of them. This is
        only possible with the request in the context.
        """
        user = get_user(self.request.user)
        filters = Q(user=user) | Q(group__in=user.groups.all())
        anonymous_user = get_anonymous_user()
        if user != anonymous_user:
            filters |= Q(user=anonymous_user)

        qs_permission_model = self.qs_permission_model.filter(filters)

        return self.queryset.prefetch_related(
            Prefetch("permission_group__permissions", queryset=qs_permission_model)
        )
