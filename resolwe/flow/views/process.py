"""Process viewset."""
from rest_framework import exceptions, mixins, viewsets

from resolwe.flow.filters import ProcessFilter
from resolwe.flow.models import Process
from resolwe.flow.serializers import ProcessSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin

from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin


class ProcessViewSet(ResolweCreateModelMixin,
                     mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     ResolwePermissionsMixin,
                     ResolweCheckSlugMixin,
                     viewsets.GenericViewSet):
    """API view for :class:`Process` objects."""

    queryset = Process.objects.all().prefetch_related('contributor')
    serializer_class = ProcessSerializer
    permission_classes = (get_permissions_class(),)
    filter_class = ProcessFilter
    ordering_fields = ('id', 'created', 'modified', 'name', 'version')
    ordering = ('id',)

    def create(self, request, *args, **kwargs):
        """Only superusers can create new processes."""
        if not request.user.is_superuser:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)
