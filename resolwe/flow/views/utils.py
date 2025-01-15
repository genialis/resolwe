"""Resolwe views utils."""

from rest_framework import exceptions, permissions

from resolwe.flow.models import Collection
from resolwe.permissions.models import Permission


def get_collection_for_user(collection_id, user):
    """Check that collection exists and user has `edit` permission."""
    collection_query = Collection.objects.filter(pk=collection_id)
    if not collection_query.exists():
        raise exceptions.ValidationError("Collection id does not exist")

    collection = collection_query.first()
    if not user.has_perm(Permission.EDIT, obj=collection):
        if user.is_authenticated:
            raise exceptions.PermissionDenied()
        else:
            raise exceptions.NotFound()

    return collection


class IsStaffOrTargetUser(permissions.BasePermission):
    """Permission class for user endpoint."""

    def has_permission(self, request, view):
        """Check if user has permission."""
        return True

    def has_object_permission(self, request, view, obj):
        """Check if user has object permission."""
        return request.user.is_staff or obj == request.user


class IsSuperuserOrReadOnly(permissions.BasePermission):
    """Superuser has permissions, otherwise only safe methods are allowed."""

    def has_permission(self, request, view):
        """Check if user has permission."""
        return request.method in permissions.SAFE_METHODS or (
            request.user and request.user.is_superuser
        )


class IsStaffOrReadOnly(permissions.BasePermission):
    """Staff user has permissions, otherwise only safe methods are allowed."""

    def has_permission(self, request, view):
        """Check if user has permission."""
        return request.method in permissions.SAFE_METHODS or (
            request.user and request.user.is_staff
        )
