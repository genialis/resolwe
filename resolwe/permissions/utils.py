""".. Ignore pydocstyle D400.

=================
Permissions utils
=================

.. autofunction:: copy_permissions

"""

from typing import Optional, Tuple, Type, Union

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group, User
from django.db import models, transaction
from rest_framework import exceptions

from resolwe.permissions.models import (
    Permission,
    PermissionInterface,
    PermissionModel,
    UserOrGroup,
    get_anonymous_user,
)


def set_permission(
    permission: Permission,
    user_or_group: UserOrGroup,
    obj: models.Model,
):
    """Set permission on the given object.

    It performs additional check to determine if permissions can be set on the
    given object using duck-typing.

    :raises AssertionError: when no user or group is given.

    :raises RuntimeError: when given object has no permission_group or is
        contained in a container.

    :raises ValueError: when the given permission can not be found.
    """
    # First perform basic checks on the object itself.
    if not model_has_permissions(obj):
        raise RuntimeError(
            f"There is no support for permissions on object of type {obj._meta.label}."
        )
    obj.set_permission(permission, user_or_group)


def get_user(user: User | AnonymousUser) -> User:
    """Get the same user or anonymous one when user is not authenticated.

    :raises django.core.exceptions.ObjectDoesNotExist: when user is not
        authenticated and anonymous user could not be found.
    """
    if user.is_authenticated:
        return user
    else:
        return get_anonymous_user()


def model_has_permissions(obj: Union[models.Model, Type[models.Model]]) -> bool:
    """Check whether model has object level permissions."""

    try:
        return isinstance(obj, PermissionInterface) or issubclass(
            obj, PermissionInterface
        )
    except TypeError:
        return False


def get_identity(user_group: Union[Group, User]) -> Tuple[UserOrGroup, str]:
    """Return the tuple (user_or_group, name).

    The user_or_group is user or group and name is either 'user' or 'group'.

    :raises RuntimeError: when parameter is neither user nor group instance.
    """
    if isinstance(user_group, AnonymousUser):
        return get_anonymous_user(), "user"

    UserModel = get_user_model()
    if isinstance(user_group, UserModel):
        return user_group, "user"

    if isinstance(user_group, Group):
        return user_group, "group"

    raise RuntimeError("Parameter is not user or group instance.")


@transaction.atomic
def copy_permissions(src_obj: models.Model, dest_obj: models.Model):
    """Copy permissions form ``src_obj`` to ``dest_obj``.

    .. warning::
        Existing permissions in dest_obj will we deleted.
    """

    # TODO: why is this here? Can this condition ever be fullfilled?
    if src_obj is None or dest_obj is None:
        return

    if not model_has_permissions(src_obj) or not model_has_permissions(dest_obj):
        return

    # When permission groups are the same there is nothing to do.
    if src_obj.permission_group == dest_obj.permission_group:
        return

    # Delete existing permissions on dest_obj.
    PermissionModel.objects.filter(permission_group=dest_obj.permission_group).delete()

    new_permissions = [
        PermissionModel(
            value=permission_model.value,
            permission_group=dest_obj.permission_group,
            user=permission_model.user,
            group=permission_model.group,
        )
        for permission_model in src_obj.permission_group.permissions.all()
    ]
    PermissionModel.objects.bulk_create(new_permissions)


def fetch_user(query: str) -> User:
    """Get user by ``pk``, ``username`` or ``email``.

    :raises ParseError: if user can not be determined.
    """
    user_filter = models.Q(username=query) | models.Q(email=query)
    if query.isdigit():
        user_filter |= models.Q(pk=query)

    user_model = get_user_model()
    try:
        return user_model.objects.get(user_filter)
    except user_model.DoesNotExist:
        raise exceptions.ParseError("Unknown user: {}".format(query))
    except user_model.MultipleObjectsReturned:
        raise exceptions.ParseError("Cannot uniquely determine user: {}".format(query))


def fetch_group(query: str) -> Group:
    """Get group by ``pk`` or ``name``. Raise error if it doesn't exist."""
    group_filter = {"pk": query} if query.isdigit() else {"name": query}

    try:
        return Group.objects.get(**group_filter)
    except Group.DoesNotExist:
        raise exceptions.ParseError("Unknown group: {}".format(query))


def check_owner_permission(payload: dict, allow_user_owner: bool, obj: models.Model):
    """Check if one can grant or revoke owner permission.

    Owner permission to user should only be granted or revoked when
    allow_user_owner is set.

    Owner permission to group should never be set.

    :attr allow_user_owner: True if owner permission can be granted or revoked.

    :raised exceptions.ParseError: when owner permission is assigned to a
        group.

    :raised exceptions.PermissionDenied: when owner permission is assigned t
        or revoked from user and allow_user_owner is not set.
    """
    for entity_type in ["users", "groups"]:
        for user_identification, permission in payload.get(entity_type, {}).items():
            if permission == "owner":
                if entity_type == "users" and not allow_user_owner:
                    raise exceptions.PermissionDenied(
                        "Only owners can grant/revoke owner permission"
                    )

                if entity_type == "groups":
                    raise exceptions.ParseError(
                        "Owner permission cannot be assigned to a group"
                    )
            # Here we have to check if owner permission is being revoked.
            # Unfortunately there is no way to do this without hitting the
            # database.
            elif entity_type == "users":
                if not allow_user_owner:
                    user = fetch_user(str(user_identification))
                    if obj.is_owner(user):
                        raise exceptions.PermissionDenied(
                            "Only owners can grant/revoke owner permission"
                        )


def check_public_permissions(payload: dict):
    """Raise ``PermissionDenied`` if public permissions are too open."""
    allowed_public_permissions = ["none", "view"]
    if "public" in payload:
        if payload["public"] not in allowed_public_permissions:
            raise exceptions.PermissionDenied(
                "Permissions for public users are too open"
            )


def check_user_permissions(payload: dict, user_pk: int):
    """Raise ``PermissionDenied`` if ``payload`` includes ``user_pk``."""
    user_pks = payload.get("users", {}).keys()
    if user_pk in user_pks:
        raise exceptions.PermissionDenied("You cannot change your own permissions")


def update_permission(obj: models.Model, data):
    """Update object permissions."""

    def apply_perm(permission_name: Optional[str], entity: UserOrGroup):
        """Set permission on the object obj to the given entity.

        If given permission does not exist, ``exceptions.ParseError`` is
        raised.

        If special keyword "ALL" passed as ``permission`` top-level permission
        is set on the given object for the given entity.

        If permission is None all permissions are revoked.

        :param entity: user or group to set permissions to.
        """
        try:
            permission = Permission.from_name(permission_name or "none")
        except KeyError:
            raise exceptions.ParseError(f"Unknown permission: {permission_name}")

        obj.set_permission(permission, entity)

    def set_permissions(entity_type):
        """Set object permissions."""
        fetch_entity = fetch_user if entity_type == "users" else fetch_group

        for entity_id in data.get(entity_type, {}):
            apply_perm(data[entity_type][entity_id], fetch_entity(str(entity_id)))

    def set_public_permissions():
        """Set public permissions."""
        if "public" in data:
            apply_perm(data["public"], get_anonymous_user())

    with transaction.atomic():
        set_permissions("users")
        set_permissions("groups")
        set_public_permissions()


def assign_contributor_permissions(obj, contributor=None):
    """Assign all permissions to object's contributor."""
    obj.set_permission(Permission.highest(), contributor or obj.contributor)
