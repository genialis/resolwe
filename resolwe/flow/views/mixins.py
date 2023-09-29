"""Mixins used in Resolwe Viewsets."""
from drf_spectacular.utils import extend_schema

from django.db import IntegrityError, transaction

from rest_framework import mixins, serializers, status
from rest_framework.decorators import action
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response

from resolwe.observers.models import BackgroundTask
from resolwe.observers.views import BackgroundTaskSerializer
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


class ResolweBackgroundDeleteMixin(mixins.DestroyModelMixin):
    """Delete a model instance in the background if possible."""

    BACKGROUND_DELETE_METHOD = "delete_background"

    def perform_destroy(self, instance):
        """Perform the actual delete.

        If possible, detele the object in the background.
        """
        return getattr(instance, self.BACKGROUND_DELETE_METHOD, "delete")()

    def destroy(self, request, *args, **kwargs):
        """Delete a resource."""
        instance = self.get_object()
        return_value = self.perform_destroy(instance)
        # When the object is deleted in the background, return a task description.
        if isinstance(return_value, BackgroundTask):
            return Response(
                status=status.HTTP_200_OK,
                data=BackgroundTaskSerializer(return_value).data,
            )
        else:
            return Response(status=status.HTTP_204_NO_CONTENT)


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


class NameSerializer(serializers.Serializer):
    """Serializer for the name field."""

    name = serializers.CharField()


class ResolweCheckSlugMixin:
    """Slug validation."""

    @extend_schema(parameters=[NameSerializer()], responses={status.HTTP_200_OK: bool})
    @action(detail=False, methods=["get"])
    def slug_exists(self, request):
        """Check if given url slug exists.

        Check if slug given in query parameter ``name`` exists. Return
        ``True`` if slug already exists and ``False`` otherwise.

        """
        if not request.user.is_authenticated:
            return Response(status=status.HTTP_401_UNAUTHORIZED)

        serializer = NameSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        slug_name = serializer.validated_data["name"]

        queryset = self.get_queryset()
        return Response(queryset.filter(slug__iexact=slug_name).exists())
