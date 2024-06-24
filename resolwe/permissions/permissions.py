"""Custom permissions for Flow API."""

from typing import Optional

from django.contrib.auth.models import User
from django.db import models
from django.http import Http404, HttpRequest
from rest_framework import exceptions, permissions

from resolwe.permissions.models import Permission, PermissionList, get_anonymous_user
from resolwe.permissions.utils import get_user, model_has_permissions


class ResolwePermissionBackend:
    """Custom resolwe permission backend."""

    supports_object_permissions = True
    supports_anonymous_user = True
    supports_inactive_user = True

    def authenticate(
        self,
        request: HttpRequest,
        username: Optional[User] = None,
        password: Optional[str] = None,
    ):
        """Authenticate user.

        Since our backend is only used for authorization return None. See

        https://docs.djangoproject.com/en/3.2/topics/auth/customizing/

        for details.
        """
        return None

    def has_perm(
        self,
        user: User,
        permission: Permission | str,
        obj: Optional[models.Model] = None,
    ) -> bool:
        """
        Check if user has the given permission on the given object.

        :returns: ``True`` if given ``user_obj`` has ``perm`` for ``obj``.
            If no ``obj`` is given or user is inactive and authenticated at the
            same time ``False`` is returned.
        """
        user = get_user(user)
        if not user.is_active:
            return False

        # We do not handle model level permissions.
        if obj is None:
            return False

        assert isinstance(permission, Permission)
        if not model_has_permissions(obj):
            return True
        else:
            return obj.has_permission(permission, user)

    def get_all_permissions(
        self, user: User, obj: Optional[models.Model] = None
    ) -> PermissionList:
        """Return a set of permissions for ``user`` on ``obj``.

        Permissions from user groups are also considered.

        When user is not active the empty set of permissions is returned.
        """
        all_permissions = []
        if user.is_active and model_has_permissions(obj):
            permission_value = obj.permission_group.permissions.filter(
                models.Q(user=user) | models.Q(group__in=user.groups.all())
            ).aggregate(permission=models.Max("value"))["permission"]
            if permission_value:
                all_permissions = list(Permission(permission_value))
        return all_permissions


class ResolwePermissions(permissions.DjangoObjectPermissions):
    """Resolwe Permissions."""

    perms_map = {
        "GET": [Permission.VIEW],
        "OPTIONS": [Permission.VIEW],
        "HEAD": [Permission.VIEW],
        "POST": [Permission.EDIT],
        "PUT": [Permission.EDIT],
        "PATCH": [Permission.EDIT],
        "DELETE": [Permission.EDIT],
    }

    def get_required_object_permissions(self, method, model_cls):
        """Return the list of permissions that the user is required to have."""
        if method not in self.perms_map:
            raise exceptions.MethodNotAllowed(method)

        return self.perms_map[method]

    def has_permission(self, request, view):
        """Return `True` as we don't use model level perms."""
        return True

    def has_object_permission(self, request, view, obj):
        """Check object permissions."""

        # admins can do anything
        if request.user.is_superuser:
            return True

        # No permission on objects that do not support it.
        if not model_has_permissions(obj):
            return False

        # `share` permission is required for editing permissions
        if view.action is not None and "permissions" in view.action:
            self.perms_map["POST"] = [Permission.SHARE]

        if hasattr(view, "get_queryset"):
            queryset = view.get_queryset()
        else:
            queryset = getattr(view, "queryset", None)

        assert queryset is not None, (
            "Cannot apply DjangoObjectPermissions on a view that "
            "does not set `.queryset` or have a `.get_queryset()` method."
        )

        model_cls = queryset.model
        user = request.user
        perm = max(self.get_required_object_permissions(request.method, model_cls))
        anonymous = get_anonymous_user()
        if not user.has_perm(perm, obj) and not anonymous.has_perm(perm, obj):
            if request.method not in permissions.SAFE_METHODS:
                # If the user does not have permissions we need to determine if
                # they have read permissions to see 403, or not, and simply see
                # a 404 response.
                read_perm = max(self.get_required_object_permissions("GET", model_cls))

                if user.has_perm(read_perm, obj):
                    return False

            raise Http404

        return True
