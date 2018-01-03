"""Mixins used in Resolwe Viewsets."""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db import IntegrityError, transaction

from guardian.utils import get_anonymous_user
from rest_framework import mixins, status
from rest_framework.decorators import list_route
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response

from resolwe.flow.models import DescriptorSchema
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import assign_contributor_permissions


def get_descriptor_schames(query, user):
    """Return filtered queryset od DescriptorSchama-s.

    Queryset is filtered  by primary key (if ``query`` parameter is of
    type ``int``` or slug (otherwise). Additionaly filter by ``VIEW``
    permission is aplied for given user ``user``.
    """
    queryset_filter = {}

    if isinstance(query, int):
        queryset_filter['pk'] = query
    else:
        queryset_filter['slug'] = query

    queryset = DescriptorSchema.objects.filter(**queryset_filter)
    queryset = get_objects_for_user(user, 'view_descriptorschema', queryset)

    return queryset


class ResolweCreateModelMixin(mixins.CreateModelMixin):
    """Mixin to support creating new `Resolwe` models.

    Extends `django_rest_framework`'s class `CreateModelMixin` with:

      * append user's id from request to posted data as `contributor`
        key
      * catch `IntegrityError`s, so we can return HTTP status 409
        instead of raising error

    """

    def create(self, request, *args, **kwargs):
        """Create a resource."""
        ds_query = request.data.get('descriptor_schema', None)
        if ds_query:
            ds_query = get_descriptor_schames(ds_query, request.user)
            try:
                request.data['descriptor_schema'] = ds_query.latest().pk
            except DescriptorSchema.DoesNotExist:
                return Response(
                    {'descriptor_schema': [
                        'Invalid descriptor_schema slug "{}" - object does not exist.'.format(ds_query)]},
                    status=status.HTTP_400_BAD_REQUEST)

        if request.user.is_anonymous:
            request.data['contributor'] = get_anonymous_user().pk
        else:
            request.data['contributor'] = request.user.pk

        try:
            return super(ResolweCreateModelMixin, self).create(request, *args, **kwargs)

        except IntegrityError as ex:
            return Response({u'error': str(ex)}, status=status.HTTP_409_CONFLICT)

    def perform_create(self, serializer):
        """Create a resource."""
        with transaction.atomic():
            instance = serializer.save()

            # Assign all permissions to the object contributor.
            assign_contributor_permissions(instance)


class ResolweUpdateModelMixin(mixins.UpdateModelMixin):
    """Mixin to support updating `Resolwe` models.

    Extends `django_rest_framework`'s class `UpdateModelMixin` with:

      * translate `descriptor_schema` field from DescriptorSchema's
        slug to its id and return 400 error Response if it doesn't
        exists

    """

    def update(self, request, *args, **kwargs):
        """Update a resource."""
        ds_query = request.data.get('descriptor_schema', None)
        if ds_query:
            ds_query = get_descriptor_schames(ds_query, request.user)
            try:
                request.data['descriptor_schema'] = ds_query.latest().pk
            except DescriptorSchema.DoesNotExist:
                return Response(
                    {'descriptor_schema': [
                        'Invalid descriptor_schema slug "{}" - object does not exist.'.format(ds_query)]},
                    status=status.HTTP_400_BAD_REQUEST)

        # NOTE: Use the original method instead when support for locking is added:
        #       https://github.com/encode/django-rest-framework/issues/4675
        # return super(ResolweUpdateModelMixin, self).update(request, *args, **kwargs)
        with transaction.atomic():
            return self._update(request, *args, **kwargs)

    # NOTE: This is a copy of rest_framework.mixins.UpdateModelMixin.update().
    #       The self.get_object() was replaced with the
    #       self.get_object_with_lock() to lock the object while updating.
    #       Use the original method when suport for locking is added:
    #       https://github.com/encode/django-rest-framework/issues/4675
    def _update(self, request, *args, **kwargs):
        """Update a resource."""
        partial = kwargs.pop('partial', False)
        # NOTE: The line below was changed.
        instance = self.get_object_with_lock()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        if getattr(instance, '_prefetched_objects_cache', None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}  # pylint: disable=protected-access

        return Response(serializer.data)

    # NOTE: This is a copy of rest_framework.generics.GenericAPIView.get_object().
    #       The select_for_update() was added to the 'queryset'
    #       to lock the object while updating.
    #       Use the original method when suport for locking is added:
    #       https://github.com/encode/django-rest-framework/issues/4675
    def get_object_with_lock(self):
        """Return the object the view is displaying."""
        queryset = self.filter_queryset(self.get_queryset())

        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg]}
        # NOTE: The line below was changed.
        obj = get_object_or_404(queryset.select_for_update(), **filter_kwargs)

        # May raise a permission denied.
        self.check_object_permissions(self.request, obj)

        return obj


class ResolweCheckSlugMixin(object):
    """Slug validation."""

    @list_route(methods=[u'get'])
    def slug_exists(self, request):
        """Check if given url slug exists.

        Check if slug given in query parameter ``name`` exists. Return
        ``True`` if slug already exists and ``False`` otherwise.

        """
        if not request.user.is_authenticated:
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        if 'name' not in request.query_params:
            return Response({'error': 'Query parameter `name` must be given.'},
                            status=status.HTTP_400_BAD_REQUEST)

        queryset = self.get_queryset()
        slug_name = request.query_params['name']
        return Response(queryset.filter(slug__iexact=slug_name).exists())
