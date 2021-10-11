""".. Ignore pydocstyle D400.

=================
Permissions utils
=================

.. autofunction:: copy_permissions

"""
from contextlib import suppress
from itertools import chain
from typing import List, Optional, Tuple, Union

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group, User
from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction

from rest_framework import exceptions

from resolwe.permissions.models import Permission, PermissionModel, PermissionObject


def get_perms(user_or_group: Union[User, Group], obj: models.Model) -> List[str]:
    """Get permissions for user/group on the given object."""
    # TODO: storage, others
    if not model_has_permissions(obj):
        raise RuntimeError(
            f"There is no support for permissions on object of type {obj._meta.label}."
        )
    if not isinstance(obj, PermissionObject):
        raise RuntimeError(f"The given model {obj} has no permission group.")

    # TODO: for storage objects this must be a Data permission group, so a
    # database hit is inevitable.
    permission_group = obj.permission_group
    entity = next(
        entity for entity in get_identity(user_or_group) if entity is not None
    )
    entity_name = "groups" if isinstance(entity, Group) else "users"

    permission_models = permission_group.permissions.filter(**{entity_name: entity})
    assert permission_models.count() <= 1, (
        f"Too many permissions on object of type {obj._meta.label} with "
        f"key {obj.pk} for {entity_name} with key {entity.pk}."
    )

    permission_model = permission_models.first()
    if permission_model is None:
        return []
    calculated_permissions = Permission(permission_model.permission).all_permissions()
    all_object_permissions = get_all_perms(obj)
    return [
        permission
        for permission in calculated_permissions
        if permission in all_object_permissions
    ]


def set_permission(
    perm: Optional[str], user_or_group: Union[User, Group], obj: models.Model
):
    """Set the given permission.

    Since permissions are totally ordered it only makes sense to set the
    permissions. The previous permissions user/group might have on the object
    are discarded.

    When None is given for perm all user permissions are removed.

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
    if obj.in_container():
        raise RuntimeError(
            f"The permissions can not be set on object {obj} ({obj._meta.label}) in container."
        )
    permission_group = obj.permission_group

    # Now check user/group.
    entity = next(
        entity for entity in get_identity(user_or_group) if entity is not None
    )
    assert entity is not None, "User or group must be given."
    entity_name = "groups" if isinstance(entity, Group) else "users"

    # Remove old permission (if they exist).
    with suppress(PermissionModel.DoesNotExist):
        permission_model = permission_group.permissions.get(**{entity_name: entity})
        getattr(permission_model, entity_name).remove(entity)

    # Assign new permission.
    if perm is not None:
        permission = Permission.from_name(perm)
        new_permission_model = PermissionModel.objects.get_or_create(
            permission=permission.value, permission_group=permission_group
        )[0]
        getattr(new_permission_model, entity_name).add(entity)


def get_user(user: User) -> User:
    """Get the same user or anonymous one when not authenticated.

    :raises django.core.exceptions.ObjectDoesNotExist: when user is not
        authenticated and anonymous user could not be found.
    """
    if user.is_authenticated:
        return user
    else:
        return get_anonymous_user()


def model_has_permissions(obj: models.Model) -> bool:
    """Check whether model has object level permissions."""
    additional_labels = ("flow.Relation", "flow.Storage")
    return hasattr(obj, "permission_group") or obj._meta.label in additional_labels


def user_model_check(user: User, obj: models.Model) -> Tuple[bool, User, models.Model]:
    """Check the user and model and decice how to perform the permission checks.

    :returns: the tuple (should_check, user, object) indicating whether permissions
        should be checked for the returned user.
    """
    # Can only check permissions on specific models.
    if not isinstance(obj, models.Model):
        return False, user, obj

    if not model_has_permissions(obj):
        return False, user, obj

    # Use the anonymous user object if user is not logged in.
    try:
        user = get_user(user)
    except ObjectDoesNotExist:
        return False, user, obj

    return True, user, obj


def get_anonymous_user() -> User:
    """Get the anonymous user.

    Note that is the actual user object with username specified in setting
    ANONYMOUS_USER_NAME or id specified in setting ANONYMOUS_USER_ID. The later
    setting has precedence.

    :raises django.core.exceptions.ObjectDoesNotExist: when anonymous
        user could not be found.
    :raises RuntimeError: when setting ANONYMOUS_USER_NAME could not be found.
    """

    anonymous_user_id = getattr(settings, "ANONYMOUS_USER_ID", None)
    if anonymous_user_id:
        try:
            return get_user_model().objects.get(id=anonymous_user_id)
        except User.DoesNotExist:
            from resolwe.test.utils import is_testing

            # Resolve circular import.
            if is_testing():
                return get_user_model().objects.create(
                    id=anonymous_user_id, username="public", is_active=True, email=""
                )
            else:
                raise

    anonymous_username = getattr(settings, "ANONYMOUS_USER_NAME", None)
    if anonymous_username:
        try:
            return get_user_model().objects.get(
                **{User.USERNAME_FIELD: anonymous_username}
            )
        except User.DoesNotExist:
            # Resolve circular import.
            from resolwe.test.utils import is_testing

            if is_testing():
                return get_user_model().objects.create(
                    username=anonymous_username, is_active=True, email=""
                )
            else:
                raise

    raise RuntimeError("No ANONYMOUS_USER_ID/ANONYMOUS_USER_NAME setting found.")


def get_identity(
    user_group: Union[Group, User]
) -> Tuple[Optional[User], Optional[Group]]:
    """Return the tuple (user, group) where exactly one of them is None.

    :raises RuntimeError: when parameter is neither user nor group instance.
    """
    if isinstance(user_group, AnonymousUser):
        return get_anonymous_user(), None

    UserModel = get_user_model()
    if isinstance(user_group, UserModel):
        return user_group, None

    if isinstance(user_group, Group):
        return None, user_group

    raise RuntimeError("Parameter is not user or group instance.")


def get_all_perms(obj):
    """Return a list of all permissions on ``obj``."""
    return list(zip(*obj._meta.permissions))[0]


def copy_permissions(src_obj: models.Model, dest_obj: models.Model):
    """Copy permissions form ``src_obj`` to ``dest_obj``."""
    # TODO: why is this here? Can this condition ever be fullfilled?
    if src_obj is None or dest_obj is None:
        return

    if not model_has_permissions(src_obj) or not model_has_permissions(dest_obj):
        return

    source_permission_group = src_obj.permission_group
    destination_permission_group = dest_obj.permission_group

    for permission_model in source_permission_group.permissions.all():
        try:
            new_permission_model = PermissionModel.objects.get(
                permission=permission_model.permission,
                permission_group=destination_permission_group,
            )
        except PermissionModel.DoesNotExist:
            new_permission_model = PermissionModel.objects.create(
                permission=permission_model.permission,
                permission_group=destination_permission_group,
            )
        finally:
            # Check for existing permission and assing one only when needed.
            # This could be slow but it is necessary in order to have only one
            # permission perm user/group per permission group, which makes
            # sense if permissions are ordered linearly.
            for entity in chain(
                permission_model.users.all(), permission_model.groups.all()
            ):
                is_user = isinstance(entity, User)
                attribute_name = "users" if is_user else "groups"
                filter = {f"{attribute_name}": entity}
                existing_permissions = destination_permission_group.permissions.filter(
                    **filter
                )
                if existing_permissions:
                    existing_permission = existing_permissions.get()
                    # Higher (or equal) permission is OK.
                    if existing_permission.permission >= permission_model.permission:
                        continue
                    # Lower has to be deleted, then added with the correct permission.
                    elif existing_permission.permission < permission_model.permission:
                        getattr(existing_permission, attribute_name).remove(entity)
                getattr(new_permission_model, attribute_name).add(entity)


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


def check_owner_permission(payload: dict, allow_user_owner: bool):
    """Raise ``PermissionDenied``if ``owner`` found in ``data``."""
    for entity_type in ["users", "groups"]:
        for permission in payload.get(entity_type, {}).values():
            if permission == "owner":
                if entity_type == "users" and allow_user_owner:
                    continue

                if entity_type == "groups":
                    raise exceptions.ParseError(
                        "Owner permission cannot be assigned to a group"
                    )

                raise exceptions.PermissionDenied(
                    "Only owners can grant/revoke owner permission"
                )


def check_public_permissions(payload: dict):
    """Raise ``PermissionDenied`` if public permissions are too open."""
    allowed_public_permissions = ["view"]
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
    full_permissions = get_all_perms(obj)

    def apply_perm(permission: str, entity: Union[User, Group]):
        """Set permission on the object obj to the given entity.

        If given permission does not exist, ``exceptions.ParseError`` is
        raised.

        If special keyword "ALL" passed as ``permission`` top-level permission
        is set on the given object for the given entity.

        If special keywore "NONE" is passed as ``permission`` all permissions
        are revoked.

        :param entity: user or group to set permissions to.
        """
        if permission == "NONE":
            obj.set_permission(None, entity)
        elif permission == "ALL":
            for current_perm in reversed(
                Permission.highest_permission().all_permission_objects()
            ):
                if str(permission) in full_permissions:
                    obj.set_permission(current_perm.value, entity)
                    break
            raise RuntimeError("Object must have at least one permission.")
        else:
            if permission not in full_permissions:
                raise exceptions.ParseError("Unknown permission: {}".format(permission))
            obj.set_permission(permission, entity)

    def set_permissions(entity_type):
        """Set object permissions."""
        fetch_entity = fetch_user if entity_type == "users" else fetch_group

        for entity_id in data.get(entity_type, {}):
            apply_perm(data[entity_type][entity_id], fetch_entity(entity_id))

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
    permission = str(Permission.highest_permission())
    set_permission(permission, contributor or obj.contributor, obj)
