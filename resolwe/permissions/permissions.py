"""Custom permissions for Flow API."""
from typing import List, Optional

from django.contrib.auth.models import User
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.http import Http404, HttpRequest

from rest_framework import permissions

from resolwe.permissions.models import Permission
from resolwe.permissions.utils import get_anonymous_user, user_model_check


def _permission_check(permission_name: str, obj: models.Model) -> str:
    """Check the app labels on permission and object.

    :raises RuntimeError: when the permission app name and model app name
        differ.
    """
    # Check if app labels match.
    # TODO: I do not understand why the second part of check is necessary.
    if permission_name is not None and "." in permission_name:
        app_label, base_permission_name = permission_name.split(".", 1)
        if app_label != obj._meta.app_label:
            # Check the content_type app_label when permission
            # and obj app labels don't match.
            ctype = ContentType.objects.get_for_model(obj)
            if app_label != ctype.app_label:
                # TODO: custom error.
                raise RuntimeError(
                    "Passed perm has app label of '%s' while "
                    "given obj has app label '%s' and given obj"
                    "content_type has app label '%s'"
                    % (app_label, obj._meta.app_label, ctype.app_label)
                )
        return base_permission_name
    return permission_name


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

        Since our backend is only used for authorization return None. For
        details see customizing authentication in Django.

        https://docs.djangoproject.com/en/3.2/topics/auth/customizing/
        """
        return None

    def has_perm(
        self, user_obj: User, perm: str, obj: Optional[models.Model] = None
    ) -> bool:
        """
        Return ``True`` if given ``user_obj`` has ``perm`` for ``obj``.

        If no ``obj`` is given, ``False`` is returned.

        .. note::

           Remember, that if user is not *active*, all checks would return
           ``False``.

        Main difference between Django's ``ModelBackend`` is that we can pass
        ``obj`` instance here and ``perm`` doesn't have to contain
        ``app_label`` as it can be retrieved from given ``obj``.

        **Inactive user support**

        If user is authenticated but inactive at the same time, all checks
        always returns ``False``.
        """
        should_check, user_obj, obj = user_model_check(user_obj, obj)
        if not should_check:
            return False

        perm = _permission_check(perm, obj)
        required_permission = Permission.from_name(perm).value

        assert hasattr(
            obj, "permission_group"
        ), f"Object of type {obj._meta.label} has no permission group field."
        userperm_queryset = obj._meta.model.objects.filter(
            pk=obj.pk,
            permission_group__permissions__users=user_obj,
            permission_group__permissions__permission__gte=required_permission,
        )
        groupperm_queryset = obj._meta.model.objects.filter(
            pk=obj.pk,
            permission_group__permissions__groups__in=user_obj.groups.all(),
            permission_group__permissions__permission__gte=required_permission,
        )
        return userperm_queryset.exists() or groupperm_queryset.exists()

    def get_all_permissions(
        self, user_obj: User, obj: Optional[models.Model] = None
    ) -> List[str]:
        """Return a set of permissions for ``user_obj`` on ``obj``."""
        should_check, user_obj, obj = user_model_check(user_obj, obj)
        if not should_check:
            return []
        permission_value = obj.permission_group.permissions.filter(
            models.Q(users=user_obj) | models.Q(groups__in=user_obj.groups.all())
        ).aggregate(permission=models.Max("permission"))["permission"]
        if permission_value:
            return Permission(permission_value).all_permissions()
        else:
            return []


class ResolwePermissions(permissions.DjangoObjectPermissions):
    """Resolwe Permissions."""

    perms_map = {
        "GET": ["%(app_label)s.view"],
        "OPTIONS": ["%(app_label)s.view"],
        "HEAD": ["%(app_label)s.view"],
        "POST": ["%(app_label)s.edit"],
        "PUT": ["%(app_label)s.edit"],
        "PATCH": ["%(app_label)s.edit"],
        "DELETE": ["%(app_label)s.edit"],
    }

    def has_permission(self, request, view):
        """Return `True` as we don't use model level perms."""
        return True

    def has_object_permission(self, request, view, obj):
        """Check object permissions."""
        # admins can do anything
        if request.user.is_superuser:
            return True

        # `share` permission is required for editing permissions
        if "permissions" in view.action:
            self.perms_map["POST"] = ["%(app_label)s.share"]

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
        # Return permissions based on the method and perms_map above. Since our
        # permissions are linearly ordered and lower are includad into higher
        # this must always only be highest possible permission.
        # Searching for more than one permission will slow things down.
        perms = self.get_required_object_permissions(request.method, model_cls)

        if not user.has_perms(perms, obj) and not get_anonymous_user().has_perms(
            perms, obj
        ):
            if request.method not in permissions.SAFE_METHODS:
                # If the user does not have permissions we need to determine if
                # they have read permissions to see 403, or not, and simply see
                # a 404 response.
                read_perms = self.get_required_object_permissions("GET", model_cls)
                if user.has_perms(read_perms, obj):
                    return False

            raise Http404

        return True
