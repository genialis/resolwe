"""Background task viewset."""

from django_filters import rest_framework as filters
from rest_framework import mixins, serializers, viewsets

from resolwe.observers.mixins import ObservableMixin
from resolwe.observers.models import BackgroundTask

STATUSES = [status[0] for status in BackgroundTask.STATUS_CHOICES]


class BackgroundTaskFilter(filters.FilterSet):
    """Filter for BackgroundTask."""

    class Meta:
        """Set the filter lookups for the status field."""

        model = BackgroundTask
        fields = {"status": ["exact", "in"], "id": ["exact", "in"]}


class BackgroundTaskSerializer(serializers.ModelSerializer):
    """Serialize the BackgroundTask model."""

    class Meta:
        """Set the model."""

        model = BackgroundTask
        fields = ("id", "started", "finished", "status", "description", "output")


class BackgroundTaksViewSet(
    ObservableMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    """Observable task viewset.

    It exposes standard observable endpoints so user can subscribe to it plus it lists
    all the background tasks for a current user.
    """

    serializer_class = BackgroundTaskSerializer
    queryset = BackgroundTask.objects.all()
    filterset_class = BackgroundTaskFilter
