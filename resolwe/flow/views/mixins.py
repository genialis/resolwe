"""Mixins used in Resolwe Viewsets."""

from django.db import IntegrityError, transaction
from drf_spectacular.utils import extend_schema
from rest_framework import exceptions, mixins, serializers, status
from rest_framework.decorators import action
from rest_framework.generics import get_object_or_404
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from resolwe.observers.models import BackgroundTask
from resolwe.observers.views import BackgroundTaskSerializer
from resolwe.permissions.models import (
    Permission,
    PermissionInterface,
    get_anonymous_user,
)
from resolwe.permissions.utils import assign_contributor_permissions


class DuplicateSerializer(serializers.Serializer):
    """Deserializer for duplicate endpoint."""

    ids = serializers.ListField(child=serializers.IntegerField(), allow_empty=False)


class DeleteSerializer(serializers.Serializer):
    """Bulk delete given objects."""

    ids = serializers.ListField(child=serializers.IntegerField(), allow_empty=False)


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
        """Define contributor by adding it to request.data.

        When data is a list, iterate over it and add the contributor to each entry.
        """
        contributor_pk = self.resolve_user(request.user).pk
        for entry in request.data if isinstance(request.data, list) else [request.data]:
            entry["contributor"] = contributor_pk

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
            if isinstance(instance, PermissionInterface):
                # Assign all permissions to the object contributor when object is not
                # in container.
                if not instance.in_container():
                    assign_contributor_permissions(instance)


class ResolweBackgroundDuplicateMixin:
    """Duplicate objects in the background."""

    # The method background queryset must have.
    BACKGROUND_DUPLICATE_METHOD = "duplicate"

    @extend_schema(
        filters=False,
        request=DuplicateSerializer(),
        responses={200: BackgroundTaskSerializer},
    )
    @action(detail=False, methods=["post"], permission_classes=[IsAuthenticated])
    def duplicate(self, request, *args, **kwargs):
        """Bulk duplicate  objects in the background.

        The background task is returned.
        """
        serializer = DuplicateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        Model = self.serializer_class.Meta.model

        # First check if the queryset supports background duplication.
        if not hasattr(Model.objects.none(), self.BACKGROUND_DUPLICATE_METHOD):
            raise exceptions.ValidationError(
                f"The model '{Model}' does not support background duplication."
            )

        ids = serializer.validated_data["ids"]
        to_duplicate = Model.objects.filter(id__in=ids).filter_for_user(request.user)
        actual_ids = to_duplicate.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            id_string = ", ".join(map(str, missing_ids))
            raise exceptions.PermissionDenied(
                f"Objects with ids {id_string} not found."
            )

        duplicate_method = getattr(to_duplicate, self.BACKGROUND_DUPLICATE_METHOD)
        task = duplicate_method(request_user=request.user)

        return Response(
            status=status.HTTP_200_OK,
            data=BackgroundTaskSerializer(task).data,
        )


class ResolweBackgroundDeleteMixin(mixins.DestroyModelMixin):
    """Delete a model instance in the background if possible."""

    BACKGROUND_DELETE_METHOD = "delete_background"

    def perform_destroy(self, instance):
        """Perform the actual delete.

        If possible, delete the object in the background.
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

    @extend_schema(
        filters=False,
        request=DeleteSerializer(),
        responses={200: BackgroundTaskSerializer},
    )
    @action(detail=False, methods=["post"])
    def bulk_delete(self, request, *args, **kwargs):
        """Bulk delete  objects in the background.

        The background task is returned.
        """
        serializer = DeleteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        ids = set(serializer.validated_data["ids"])

        # The list of ids must not be empty.
        if not ids:
            raise exceptions.ValidationError("No ids provided.")

        queryset = self.get_queryset()
        if not getattr(queryset, self.BACKGROUND_DELETE_METHOD, None):
            raise exceptions.ValidationError(
                f"Model {queryset.model} does not support background deletion."
            )

        can_view = queryset.filter(id__in=ids).filter_for_user(
            request.user, Permission.VIEW
        )
        can_delete = can_view.filter_for_user(request.user, Permission.EDIT)
        can_view_ids = set(can_view.values_list("id", flat=True))
        can_delete_ids = set(can_delete.values_list("id", flat=True))

        if missing_ids := can_view_ids - can_delete_ids:
            joined_ids = ", ".join(map(str, missing_ids))
            raise exceptions.PermissionDenied(
                f"No permission to delete objects with ids {joined_ids}."
            )

        # User can not delete any object.
        if not can_delete_ids:
            raise exceptions.PermissionDenied(
                f"No permission to delete objects with ids {ids}."
            )

        # Start the background task.
        task = getattr(can_delete, self.BACKGROUND_DELETE_METHOD, None)(request.user)
        return Response(
            status=status.HTTP_200_OK,
            data=BackgroundTaskSerializer(task).data,
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
