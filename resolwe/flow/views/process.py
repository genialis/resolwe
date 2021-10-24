"""Process viewset."""
from django.db.models import Prefetch, Q

from rest_framework import exceptions, mixins, viewsets

from resolwe.flow.filters import ProcessFilter
from resolwe.flow.models import Process
from resolwe.flow.serializers import ProcessSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.models import PermissionModel
from resolwe.permissions.utils import get_anonymous_user, get_user

from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin


class ProcessViewSet(
    ResolweCreateModelMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    ResolwePermissionsMixin,
    ResolweCheckSlugMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`Process` objects."""

    qs_permission_model = PermissionModel.objects.select_related("user", "group")
    queryset = Process.objects.all().select_related("contributor")
    serializer_class = ProcessSerializer
    permission_classes = (get_permissions_class(),)
    filterset_class = ProcessFilter
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

    def create(self, request, *args, **kwargs):
        """Only superusers can create new processes."""
        if not request.user.is_superuser:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)
