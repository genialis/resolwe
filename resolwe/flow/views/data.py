"""Data viewset."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import transaction

from guardian.shortcuts import assign_perm
from rest_framework import exceptions, mixins, status, viewsets
from rest_framework.decorators import list_route
from rest_framework.response import Response

from resolwe.flow.filters import DataFilter
from resolwe.flow.managers import manager
from resolwe.flow.models import Collection, Data, Process
from resolwe.flow.serializers import DataSerializer
from resolwe.flow.utils import dict_dot, get_data_checksum, iterate_schema
from resolwe.permissions.loader import get_permissions_class
from resolwe.permissions.mixins import ResolwePermissionsMixin
from resolwe.permissions.shortcuts import get_objects_for_user

from .mixins import ResolweCheckSlugMixin, ResolweCreateModelMixin, ResolweUpdateModelMixin


class DataViewSet(ResolweCreateModelMixin,
                  mixins.RetrieveModelMixin,
                  ResolweUpdateModelMixin,
                  mixins.DestroyModelMixin,
                  mixins.ListModelMixin,
                  ResolwePermissionsMixin,
                  ResolweCheckSlugMixin,
                  viewsets.GenericViewSet):
    """API view for :class:`Data` objects."""

    queryset = Data.objects.all().prefetch_related('process', 'descriptor_schema', 'contributor')
    serializer_class = DataSerializer
    permission_classes = (get_permissions_class(),)
    filter_class = DataFilter
    ordering_fields = ('id', 'created', 'modified', 'started', 'finished', 'name')
    ordering = ('id',)

    def create(self, request, *args, **kwargs):
        """Create a resource."""
        collections = request.data.get('collections', [])

        # check that user has permissions on all collections that Data
        # object will be added to
        for collection_id in collections:
            try:
                collection = Collection.objects.get(pk=collection_id)
            except Collection.DoesNotExist:
                return Response({'collections': ['Invalid pk "{}" - object does not exist.'.format(collection_id)]},
                                status=status.HTTP_400_BAD_REQUEST)

            if not request.user.has_perm('add_collection', obj=collection):
                if request.user.has_perm('view_collection', obj=collection):
                    raise exceptions.PermissionDenied(
                        "You don't have `ADD` permission on collection (id: {}).".format(collection_id)
                    )
                else:
                    raise exceptions.NotFound(
                        "Collection not found (id: {}).".format(collection_id)
                    )

        # translate processe's slug to id
        process_slug = request.data.get('process', None)
        process_query = Process.objects.filter(slug=process_slug)
        process_query = get_objects_for_user(request.user, 'view_process', process_query)
        try:
            process = process_query.latest()
        except Process.DoesNotExist:
            return Response({'process': ['Invalid process slug "{}" - object does not exist.'.format(process_slug)]},
                            status=status.HTTP_400_BAD_REQUEST)
        request.data['process'] = process.pk

        # perform "get_or_create" if requested - return existing object
        # if found
        if kwargs.pop('get_or_create', False):
            process_input = request.data.get('input', {})

            # use default values if they are not given
            for field_schema, fields, path in iterate_schema(process_input, process.input_schema):
                if 'default' in field_schema and field_schema['name'] not in fields:
                    dict_dot(process_input, path, field_schema['default'])

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

        # create the objects
        resp = super(DataViewSet, self).create(request, *args, **kwargs)

        # run manager
        manager.communicate()

        return resp

    @list_route(methods=[u'post'])
    def get_or_create(self, request, *args, **kwargs):
        """Get ``Data`` object if similar already exists, otherwise create it."""
        kwargs['get_or_create'] = True
        return self.create(request, *args, **kwargs)

    def perform_create(self, serializer):
        """Create a resource."""
        with transaction.atomic():
            instance = serializer.save()

            # Assign all permissions to the object contributor.
            for permission in list(zip(*instance._meta.permissions))[0]:  # pylint: disable=protected-access
                assign_perm(permission, instance.contributor, instance)

            # Assign data object to all specified collections.
            collections = self.request.data.get('collections', [])
            for c in collections:
                collection = Collection.objects.get(pk=c)
                collection.data.add(instance)
