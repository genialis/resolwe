"""Storage viewset."""
from rest_framework import mixins, viewsets

from resolwe.flow.models import Storage
from resolwe.flow.serializers import StorageSerializer


class StorageViewSet(mixins.RetrieveModelMixin,
                     mixins.ListModelMixin,
                     viewsets.GenericViewSet):
    """API view for :class:`Storage` objects."""

    queryset = Storage.objects.all().prefetch_related('contributor')
    serializer_class = StorageSerializer
    filter_fields = ('contributor', 'name', 'created', 'modified', 'slug')
