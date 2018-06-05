"""Relation viewset."""
from itertools import zip_longest

from django.db import IntegrityError, transaction

from rest_framework import exceptions, mixins, permissions, status, viewsets
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from resolwe.flow.models import Relation
from resolwe.flow.models.entity import PositionInRelation, RelationType
from resolwe.flow.serializers import PositionInRelationSerializer, RelationSerializer
from resolwe.permissions.utils import assign_contributor_permissions


class RelationViewSet(mixins.CreateModelMixin,
                      mixins.ListModelMixin,
                      mixins.RetrieveModelMixin,
                      mixins.UpdateModelMixin,
                      mixins.DestroyModelMixin,
                      viewsets.GenericViewSet):
    """API view for :class:`Relation` objects."""

    queryset = Relation.objects.all().prefetch_related('contributor')
    serializer_class = RelationSerializer
    permission_classes = (permissions.IsAuthenticated,)
    ordering_fields = ('id', 'created', 'modified')
    ordering = ('id',)

    def _filter_queryset(self, queryset):
        """Filter queryset by queryparameters.

        Filtering is supported by ``name``, ``collection``, ``entities``
        and ``positions``.

        If ``positions`` parameter is given, it is combined with
        coresponding id in ``samples`` parameter and relations are
        filtered by (sample, position) pairs. Because of this, if
        ``positions`` is given, also ``samples`` must be given and
        they must be of the same length.

        NOTE: Because of complex filtering requirements it is not
              possible to use django_restframework_filters (at least
              not in a straight foreward way)
        """
        # query_params must be casted to dict, otherwise list values cannot be retrieved
        query_params = dict(self.request.query_params)

        id_ = query_params.get('id', None)
        relation_type = query_params.get('type', None)
        label = query_params.get('label', None)
        entities = query_params.get('entity', None)
        positions = query_params.get('position', None)
        collection = query_params.get('collection', None)

        if id_:
            queryset = queryset.filter(id=id_[0])

        if relation_type:
            queryset = queryset.filter(type__name=relation_type[0])

        if label:
            queryset = queryset.filter(label=label[0])

        if positions is not None and (entities is None or len(positions) != len(entities)):
            raise exceptions.ParseError(
                'If `positions` query parameter is given, also `entities` '
                'must be given and they must be of the same length.'
            )

        if collection:
            queryset = queryset.filter(collection__pk=collection[0])

        if entities:
            for entity, position in zip_longest(entities, positions or []):
                filter_params = {'entities__pk': entity}
                if position:
                    filter_params['positioninrelation__position'] = position
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

        relation_type = request.data.get('type')
        if not relation_type:
            return Response({'type': ['This field is required.']}, status=status.HTTP_400_BAD_REQUEST)

        rel_type_query = RelationType.objects.filter(name=relation_type)
        try:
            request.data['type'] = rel_type_query.last().pk
        except RelationType.DoesNotExist:
            return Response(
                {'type': ['Invalid type name "{}" - object does not exist.'.format(relation_type)]},
                status=status.HTTP_400_BAD_REQUEST)

        request.data['contributor'] = user.pk

        try:
            return super().create(request, *args, **kwargs)

        except IntegrityError as ex:
            return Response({'error': str(ex)}, status=status.HTTP_409_CONFLICT)

    def perform_create(self, serializer):
        """Create a relation."""
        with transaction.atomic():
            instance = serializer.save()

            assign_contributor_permissions(instance)

    @detail_route(methods=['post'])
    def add_entity(self, request, pk=None):
        """Add ``Entity`` to ``Relation``."""
        relation = self.get_object()
        serializer = PositionInRelationSerializer(data=request.data, many=True)
        if not serializer.is_valid():
            Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        serializer.save(relation=relation)

        return Response()

    @detail_route(methods=['post'])
    def remove_entity(self, request, pk=None):
        """Remove data from collection."""
        if 'ids' not in request.data:
            return Response({"error": "`ids` parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        relation = self.get_object()
        for entity_id in request.data['ids']:
            PositionInRelation.objects.filter(relation=relation, entity=entity_id).delete()

        return Response()
