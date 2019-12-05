"""Collection viewset."""
from elasticsearch_dsl.query import Q

from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.elastic.composer import composer
from resolwe.elastic.viewsets import ElasticSearchCombinedViewSet
from resolwe.flow.models import Collection
from resolwe.flow.serializers import CollectionSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import update_permission

from ..elastic_indexes import CollectionDocument
from .mixins import ParametersMixin, ResolweCheckSlugMixin, ResolweCreateModelMixin, ResolweUpdateModelMixin


class CollectionViewSet(ElasticSearchCombinedViewSet,
                        ResolweCreateModelMixin,
                        mixins.RetrieveModelMixin,
                        ResolweUpdateModelMixin,
                        mixins.DestroyModelMixin,
                        mixins.ListModelMixin,
                        ResolwePermissionsMixin,
                        ResolweCheckSlugMixin,
                        ParametersMixin,
                        viewsets.GenericViewSet):
    """API view for :class:`Collection` objects."""

    queryset = Collection.objects.all().prefetch_related('descriptor_schema', 'contributor')
    serializer_class = CollectionSerializer
    permission_classes = (get_permissions_class(),)
    document_class = CollectionDocument

    filtering_fields = (
        'id', 'slug', 'name', 'created', 'modified', 'contributor', 'owners', 'text', 'tags',
    )
    filtering_map = {
        'name': 'name.raw',
        'contributor': 'contributor_id',
        'owners': 'owner_ids',
    }
    ordering_fields = ('id', 'created', 'modified', 'name', 'contributor')
    ordering_map = {
        'name': 'name.raw',
        'contributor': 'contributor_sort',
    }
    ordering = 'id'

    def custom_filter_tags(self, value, search):
        """Support tags query."""
        if not isinstance(value, list):
            value = value.split(',')

        filters = [Q('match', **{'tags': item}) for item in value]
        search = search.query('bool', must=filters)

        return search

    def custom_filter_text(self, value, search):
        """Support general query using the 'text' attribute."""
        if isinstance(value, list):
            value = ' '.join(value)

        should = [
            Q('match', slug={'query': value, 'operator': 'and', 'boost': 10.0}),
            Q('match', **{'slug.ngrams': {'query': value, 'operator': 'and', 'boost': 5.0}}),
            Q('match', name={'query': value, 'operator': 'and', 'boost': 10.0}),
            Q('match', **{'name.ngrams': {'query': value, 'operator': 'and', 'boost': 5.0}}),
            Q('match', contributor_name={'query': value, 'operator': 'and', 'boost': 5.0}),
            Q('match', **{'contributor_name.ngrams': {'query': value, 'operator': 'and', 'boost': 2.0}}),
            Q('match', owner_names={'query': value, 'operator': 'and', 'boost': 5.0}),
            Q('match', **{'owner_names.ngrams': {'query': value, 'operator': 'and', 'boost': 2.0}}),
            Q('match', descriptor_data={'query': value, 'operator': 'and'}),
            Q('match', description={'query': value, 'operator': 'and'}),
        ]

        # Add registered text extensions.
        for extension in composer.get_extensions(self):
            if hasattr(extension, 'text_filter'):
                should += extension.text_filter(value)

        search = search.query('bool', should=should)

        return search

    def set_content_permissions(self, user, obj, payload):
        """Apply permissions to data objects and entities in ``Collection``."""
        for entity in obj.entity_set.all():
            if user.has_perm('share_entity', entity):
                update_permission(entity, payload)

        for data in obj.data.all():
            if user.has_perm('share_data', data):
                update_permission(data, payload)

    def create(self, request, *args, **kwargs):
        """Only authenticated users can create new collections."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)

    @action(detail=False, methods=['post'])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Collection`` models."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        ids = self.get_ids(request.data)
        queryset = get_objects_for_user(request.user, 'view_collection', Collection.objects.filter(id__in=ids))
        actual_ids = queryset.values_list('id', flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Collections with the following ids not found: {}".format(', '.join(map(str, missing_ids)))
            )

        duplicated = queryset.duplicate(contributor=request.user)

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)
