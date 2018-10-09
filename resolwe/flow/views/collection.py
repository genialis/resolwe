"""Collection viewset."""
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module

from elasticsearch_dsl.query import Q

from django.db.models.query import Prefetch

from rest_framework import exceptions, mixins, status, viewsets
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from resolwe.elastic.composer import composer
from resolwe.elastic.viewsets import ElasticSearchCombinedViewSet
from resolwe.flow.models import Collection, Data
from resolwe.flow.serializers import CollectionSerializer
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.utils import remove_permission, update_permission

from ..elastic_indexes import CollectionDocument
from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin, ResolweUpdateModelMixin


class CollectionViewSet(ElasticSearchCombinedViewSet,
                        ResolweCreateModelMixin,
                        mixins.RetrieveModelMixin,
                        ResolweUpdateModelMixin,
                        mixins.DestroyModelMixin,
                        mixins.ListModelMixin,
                        ResolwePermissionsMixin,
                        ResolweCheckSlugMixin,
                        viewsets.GenericViewSet):
    """API view for :class:`Collection` objects."""

    queryset = Collection.objects.all().prefetch_related(
        'descriptor_schema',
        'contributor',
        Prefetch('data', queryset=Data.objects.all().order_by('id'))
    )
    serializer_class = CollectionSerializer
    permission_classes = (get_permissions_class(),)
    document_class = CollectionDocument

    filtering_fields = (
        'id', 'slug', 'name', 'created', 'modified', 'contributor', 'owners', 'text', 'tags',
    )
    filtering_map = {
        'name': 'name.ngrams',
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

    def get_always_allowed_arguments(self):
        """Return query arguments which are always allowed."""
        return super().get_always_allowed_arguments() + [
            'hydrate_data',
        ]

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

        # Data doesn't have "ADD" permission, so it has to be removed
        payload = remove_permission(payload, 'add')

        for data in obj.data.all():
            if user.has_perm('share_data', data):
                update_permission(data, payload)

    def create(self, request, *args, **kwargs):
        """Only authenticated usesr can create new collections."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        return super().create(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        """Destroy a model instance.

        If ``delete_content`` flag is set in query parameters, also all
        Data objects and Entities, on which user has ``EDIT``
        permission, contained in collection will be deleted.
        """
        obj = self.get_object()
        user = request.user

        if strtobool(request.query_params.get('delete_content', 'false')):
            for entity in obj.entity_set.all():
                if user.has_perm('edit_entity', entity):
                    entity.delete()

            for data in obj.data.all():
                if user.has_perm('edit_data', data):
                    data.delete()

        return super().destroy(request, *args, **kwargs)  # pylint: disable=no-member

    @detail_route(methods=['post'])
    def add_data(self, request, pk=None):
        """Add data to collection."""
        collection = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids`parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        missing = []
        for data_id in request.data['ids']:
            if not Data.objects.filter(pk=data_id).exists():
                missing.append(data_id)

        if missing:
            return Response(
                {"error": "Data objects with following ids are missing: {}".format(', '.join(missing))},
                status=status.HTTP_400_BAD_REQUEST)

        for data_id in request.data['ids']:
            collection.data.add(data_id)

        return Response()

    @detail_route(methods=['post'])
    def remove_data(self, request, pk=None):
        """Remove data from collection."""
        collection = self.get_object()

        if 'ids' not in request.data:
            return Response({"error": "`ids`parameter is required"}, status=status.HTTP_400_BAD_REQUEST)

        for data_id in request.data['ids']:
            collection.data.remove(data_id)

        return Response()
