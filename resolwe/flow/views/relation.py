"""Relation viewset."""

from itertools import zip_longest

from django.db.models import Prefetch
from rest_framework import exceptions, permissions, status, viewsets
from rest_framework.response import Response

from resolwe.flow.filters import RelationFilter
from resolwe.flow.models import DescriptorSchema, Relation
from resolwe.flow.serializers import RelationSerializer
from resolwe.permissions.models import Permission

from .collection import BaseCollectionViewSet
from .mixins import ResolweCreateModelMixin


class RelationViewSet(ResolweCreateModelMixin, viewsets.ModelViewSet):
    """API view for :class:`Relation` objects."""

    qs_collection_ds = DescriptorSchema.objects.select_related("contributor")

    queryset = (
        Relation.objects.all()
        .select_related("contributor", "type")
        .prefetch_related(
            Prefetch("collection", queryset=BaseCollectionViewSet.queryset),
            "relationpartition_set",
        )
    )

    serializer_class = RelationSerializer
    permission_classes = (permissions.AllowAny,)
    filterset_class = RelationFilter
    ordering_fields = ("id", "created", "modified")
    ordering = ("id",)

    def get_queryset(self):
        """Filter queryset by entity, label and position.

        Due to a bug in django-filter these filters have to be applied
        manually:
        https://github.com/carltongibson/django-filter/issues/883
        """
        queryset = self.queryset
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

    def _has_edit_permission(self, user, collection):
        """Return True, if user has edit permission on the collection.

        If user has no view permission it returns the HTTP status code that
        shoud be sent back to the user.
        """
        if not user.has_perm(Permission.VIEW, collection):
            return status.HTTP_404_NOT_FOUND

        elif not user.has_perm(Permission.EDIT, collection):
            return (
                status.HTTP_403_FORBIDDEN
                if user.is_authenticated
                else status.HTTP_401_UNAUTHORIZED
            )
        else:
            return True

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

        if not request.user.is_superuser:
            edit_status = self._has_edit_permission(request.user, instance.collection)
            if edit_status is not True:
                return Response(status=edit_status)

        return super().update(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        """Delete the ``Relation`` object.

        Reject the delete if user doesn't have ``EDIT`` permission on
        the collection referenced in the ``Relation``.
        """
        instance = self.get_object()

        if not request.user.is_superuser:
            edit_status = self._has_edit_permission(request.user, instance.collection)
            if edit_status is not True:
                return Response(status=edit_status)

        return super().destroy(request, *args, **kwargs)
