"""Relation viewset."""
from itertools import zip_longest

from django.db.models import Prefetch

from rest_framework import exceptions, permissions, status, viewsets
from rest_framework.response import Response

from resolwe.flow.filters import RelationFilter
from resolwe.flow.models import Collection, DescriptorSchema, Relation
from resolwe.flow.serializers import RelationSerializer

from .mixins import ResolweCreateModelMixin


class RelationViewSet(ResolweCreateModelMixin, viewsets.ModelViewSet):
    """API view for :class:`Relation` objects."""

    qs_collection_ds = DescriptorSchema.objects.select_related("contributor")
    qs_collection = Collection.objects.select_related("contributor")
    qs_collection = qs_collection.prefetch_related(
        "data", "entity_set", Prefetch("descriptor_schema", queryset=qs_collection_ds),
    )

    queryset = (
        Relation.objects.all()
        .select_related("contributor", "type")
        .prefetch_related(
            Prefetch("collection", queryset=qs_collection), "relationpartition_set"
        )
    )
    serializer_class = RelationSerializer
    permission_classes = (permissions.IsAuthenticated,)
    filterset_class = RelationFilter
    ordering_fields = ("id", "created", "modified")
    ordering = ("id",)

    def _filter_queryset(self, queryset):
        """Filter queryset by entity, label and position.

        Due to a bug in django-filter these filters have to be applied
        manually:
        https://github.com/carltongibson/django-filter/issues/883
        """
        entities = self.request.query_params.getlist("entity")
        labels = self.request.query_params.getlist("label")
        positions = self.request.query_params.getlist("position")

        if labels and len(labels) != len(entities):
            raise exceptions.ParseError(
                "If `labels` query parameter is given, also `entities` "
                "must be given and they must be of the same length."
            )

        if positions and len(positions) != len(entities):
            raise exceptions.ParseError(
                "If `positions` query parameter is given, also `entities` "
                "must be given and they must be of the same length."
            )

        if entities:
            for entity, label, position in zip_longest(entities, labels, positions):
                filter_params = {"entities__pk": entity}
                if label:
                    filter_params["relationpartition__label"] = label
                if position:
                    filter_params["relationpartition__position"] = position

                queryset = queryset.filter(**filter_params)

        return queryset

    def get_queryset(self):
        """Get queryset and perform custom filtering."""
        return self._filter_queryset(self.queryset)

    def create(self, request, *args, **kwargs):
        """Create a resource."""
        user = request.user
        if not user.is_authenticated:
            raise exceptions.NotFound

        self.define_contributor(request)

        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        """Update the ``Relation`` object.

        Reject the update if user doesn't have ``EDIT`` permission on
        the collection referenced in the ``Relation``.
        """
        instance = self.get_object()
        if (
            not request.user.has_perm("edit_collection", instance.collection)
            and not request.user.is_superuser
        ):
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        """Delete the ``Relation`` object.

        Reject the delete if user doesn't have ``EDIT`` permission on
        the collection referenced in the ``Relation``.
        """
        instance = self.get_object()

        if (
            not request.user.has_perm("edit_collection", instance.collection)
            and not request.user.is_superuser
        ):
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        return super().destroy(request, *args, **kwargs)
