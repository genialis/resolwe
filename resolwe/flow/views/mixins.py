"""Mixins used in Resolwe Viewsets."""
from django.db import IntegrityError, transaction

from rest_framework import mixins, status
from rest_framework.decorators import action
from rest_framework.exceptions import ParseError
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response

from resolwe.observers.protocol import post_permission_changed
from resolwe.permissions.models import get_anonymous_user
from resolwe.permissions.utils import assign_contributor_permissions


class ResolweCreateModelMixin(mixins.CreateModelMixin):
    """Mixin to support creating new `Resolwe` models.

    Extends `django_rest_framework`'s class `CreateModelMixin` with:

      * append user's id from request to posted data as `contributor`
        key
      * catch `IntegrityError`s, so we can return HTTP status 409
        instead of raising error

    """

    def resolve_user(self, user):
        """Resolve user instance from request."""
        return get_anonymous_user() if user.is_anonymous else user

    def define_contributor(self, request):
        """Define contributor by adding it to request.data."""
        request.data["contributor"] = self.resolve_user(request.user).pk

    def create(self, request, *args, **kwargs):
        """Create a resource."""
        self.define_contributor(request)

        try:
            return super().create(request, *args, **kwargs)

        except IntegrityError as ex:
            return Response({"error": str(ex)}, status=status.HTTP_409_CONFLICT)

    def perform_create(self, serializer):
        """Create a resource."""
        with transaction.atomic():
            instance = serializer.save()
            if hasattr(instance, "permission_group"):
                # Assign all permissions to the object contributor when object is not
                # in container.
                if not instance.in_container():
                    assign_contributor_permissions(instance)
                # The object was created inheriting permissions from its container.
                # Notify observers.
                else:
                    post_permission_changed.send(
                        sender=type(instance), instance=instance
                    )


class ResolweUpdateModelMixin(mixins.UpdateModelMixin):
    """Mixin to support updating `Resolwe` models.

    Extends `django_rest_framework`'s class `UpdateModelMixin` with:
    """

    def update(self, request, *args, **kwargs):
        """Update a resource."""
        # NOTE: Use the original method instead when support for locking is added:
        #       https://github.com/encode/django-rest-framework/issues/4675
        # return super().update(request, *args, **kwargs)
        with transaction.atomic():
            return self._update(request, *args, **kwargs)

    # NOTE: This is a copy of rest_framework.mixins.UpdateModelMixin.update().
    #       The self.get_object() was replaced with the
    #       self.get_object_with_lock() to lock the object while updating.
    #       Use the original method when suport for locking is added:
    #       https://github.com/encode/django-rest-framework/issues/4675
    def _update(self, request, *args, **kwargs):
        """Update a resource."""
        partial = kwargs.pop("partial", False)
        # NOTE: The line below was changed.
        instance = self.get_object_with_lock()
        serializer = self.get_serializer(instance, data=request.data, partial=partial)
        serializer.is_valid(raise_exception=True)
        self.perform_update(serializer)

        if getattr(instance, "_prefetched_objects_cache", None):
            # If 'prefetch_related' has been applied to a queryset, we need to
            # forcibly invalidate the prefetch cache on the instance.
            instance._prefetched_objects_cache = {}

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
            "Expected view %s to be called with a URL keyword argument "
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            "attribute on the view correctly."
            % (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = {self.lookup_field: self.kwargs[lookup_url_kwarg]}
        # NOTE: The line below was changed.
        obj = get_object_or_404(queryset.select_for_update(), **filter_kwargs)

        # May raise a permission denied.
        self.check_object_permissions(self.request, obj)

        return obj


class ResolweCheckSlugMixin:
    """Slug validation."""

    @action(detail=False, methods=["get"])
    def slug_exists(self, request):
        """Check if given url slug exists.

        Check if slug given in query parameter ``name`` exists. Return
        ``True`` if slug already exists and ``False`` otherwise.

        """
        if not request.user.is_authenticated:
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        if "name" not in request.query_params:
            return Response(
                {"error": "Query parameter `name` must be given."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        queryset = self.get_queryset()
        slug_name = request.query_params["name"]
        return Response(queryset.filter(slug__iexact=slug_name).exists())


class ParametersMixin:
    """Mixin for viewsets for handling various parameters."""

    def get_ids(self, request_data, parameter_name="ids"):
        """Extract a list of integers from request data."""
        if parameter_name not in request_data:
            raise ParseError("`{}` parameter is required".format(parameter_name))

        ids = request_data.get(parameter_name)
        if not isinstance(ids, list):
            raise ParseError("`{}` parameter not a list".format(parameter_name))

        if not ids:
            raise ParseError("`{}` parameter is empty".format(parameter_name))

        if any(map(lambda id: not isinstance(id, int), ids)):
            raise ParseError(
                "`{}` parameter contains non-integers".format(parameter_name)
            )

        return ids

    def get_id(self, request_data, parameter_name="id"):
        """Extract an integer from request data."""
        if parameter_name not in request_data:
            raise ParseError("`{}` parameter is required".format(parameter_name))

        id_parameter = request_data.get(parameter_name, None)
        if not isinstance(id_parameter, int):
            raise ParseError("`{}` parameter not an integer".format(parameter_name))

        return id_parameter
