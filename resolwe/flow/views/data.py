"""Data viewset."""
from elasticsearch_dsl.query import Q

from django.db import transaction
from django.db.models import Count

from rest_framework import exceptions, mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.elastic.composer import composer
from resolwe.elastic.viewsets import ElasticSearchCombinedViewSet
from resolwe.flow.models import Collection, Data, Entity, Process
from resolwe.flow.models.utils import fill_with_defaults
from resolwe.flow.serializers import DataSerializer
from resolwe.flow.utils import get_data_checksum
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import assign_contributor_permissions, copy_permissions

from ..elastic_indexes import DataDocument
from .mixins import ParametersMixin, ResolweCheckSlugMixin, ResolweCreateModelMixin, ResolweUpdateModelMixin


class DataViewSet(ElasticSearchCombinedViewSet,
                  ResolweCreateModelMixin,
                  mixins.RetrieveModelMixin,
                  ResolweUpdateModelMixin,
                  mixins.DestroyModelMixin,
                  ResolwePermissionsMixin,
                  ResolweCheckSlugMixin,
                  ParametersMixin,
                  viewsets.GenericViewSet):
    """API view for :class:`Data` objects."""

    queryset = Data.objects.all().prefetch_related('process', 'descriptor_schema', 'contributor', 'entity_set')
    serializer_class = DataSerializer
    permission_classes = (get_permissions_class(),)
    document_class = DataDocument

    filtering_fields = ('id', 'slug', 'version', 'name', 'created', 'modified', 'contributor', 'owners',
                        'status', 'process', 'process_type', 'type', 'process_name', 'tags', 'collection',
                        'parents', 'children', 'entity', 'started', 'finished', 'text')
    filtering_map = {
        'name': 'name.raw',
        'contributor': 'contributor_id',
        'owners': 'owner_ids',
        'process_name': 'process_name.ngrams',
    }
    ordering_fields = ('id', 'created', 'modified', 'started', 'finished', 'name', 'contributor',
                       'process_name', 'process_type', 'type')
    ordering_map = {
        'name': 'name.raw',
        'process_type': 'process_type.raw',
        'type': 'type.raw',
        'process_name': 'process_name.raw',
        'contributor': 'contributor_sort',
    }
    ordering = '-created'

    def get_always_allowed_arguments(self):
        """Return query arguments which are always allowed."""
        return super().get_always_allowed_arguments() + [
            'hydrate_data',
            'hydrate_collections',
            'hydrate_entities',
        ]

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
            Q('match', process_name={'query': value, 'operator': 'and', 'boost': 5.0}),
            Q('match', **{'process_name.ngrams': {'query': value, 'operator': 'and', 'boost': 2.0}}),
            Q('match', status={'query': value, 'operator': 'and', 'boost': 2.0}),
            Q('match', type={'query': value, 'operator': 'and', 'boost': 2.0}),
        ]

        # Add registered text extensions.
        for extension in composer.get_extensions(self):
            if hasattr(extension, 'text_filter'):
                should += extension.text_filter(value)

        search = search.query('bool', should=should)

        return search

    def create(self, request, *args, **kwargs):
        """Create a resource."""
        if kwargs.pop('get_or_create', False):
            response = self.perform_get_or_create(request, *args, **kwargs)
            if response:
                return response

        return super().create(request, *args, **kwargs)

    @action(detail=False, methods=['post'])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Data`` objects."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        ids = self.get_ids(request.data)
        queryset = get_objects_for_user(request.user, 'view_data', Data.objects.filter(id__in=ids))
        actual_ids = queryset.values_list('id', flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Data objects with the following ids not found: {}".format(', '.join(map(str, missing_ids)))
            )

        # TODO support ``inherit_collections``
        duplicated = queryset.duplicate(contributor=request.user)

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)

    @action(detail=False, methods=['post'])
    def get_or_create(self, request, *args, **kwargs):
        """Get ``Data`` object if similar already exists, otherwise create it."""
        kwargs['get_or_create'] = True
        return self.create(request, *args, **kwargs)

    def perform_get_or_create(self, request, *args, **kwargs):
        """Perform "get_or_create" - return existing object if found."""
        self.define_contributor(request)
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        process = serializer.validated_data.get('process')
        process_input = request.data.get('input', {})

        fill_with_defaults(process_input, process.input_schema)

        checksum = get_data_checksum(process_input, process.slug, process.version)
        data_qs = Data.objects.filter(
            checksum=checksum,
            process__persistence__in=[Process.PERSISTENCE_CACHED, Process.PERSISTENCE_TEMP],
        )
        data_qs = get_objects_for_user(request.user, 'view_data', data_qs)
        if data_qs.exists():
            data = data_qs.order_by('created').last()
            serializer = self.get_serializer(data)
            return Response(serializer.data)

    def perform_create(self, serializer):
        """Create a resource."""
        with transaction.atomic():
            instance = serializer.save()

            assign_contributor_permissions(instance)

            # Entity is added to the collection only when it is
            # created - when it only contains 1 Data object.
            entities = Entity.objects.annotate(num_data=Count('data')).filter(data=instance, num_data=1)

            # Assign data object to all specified collections.
            collection_pks = self.request.data.get('collections', [])
            for collection in Collection.objects.filter(pk__in=collection_pks):
                collection.data.add(instance)
                copy_permissions(collection, instance)

                # Add entities to which data belongs to the collection.
                for entity in entities:
                    entity.collections.add(collection)
                    copy_permissions(collection, entity)
