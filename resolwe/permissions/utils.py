""".. Ignore pydocstyle D400.

=================
Permissions utils
=================

.. autofunction:: copy_permissions

"""
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from django.db.models import Q

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, remove_perm
from rest_framework import exceptions


def get_perm_action(perm):
    """Split action from permission of format.

    Input permission can be in the form of '<action>_<object_type>'
    or '<action>'.
    """
    return perm.split("_", 1)[0]


def get_full_perm(perm, obj):
    """Join action with the content type of ``obj``.

    Permission is returned in the format of ``<action>_<object_type>``.
    """
    ctype = ContentType.objects.get_for_model(obj).name
    # Camel case class names are converted into a space-separated
    # content types, so spaces have to be removed.
    ctype = str(ctype).replace(" ", "")

    return "{}_{}".format(perm.lower(), ctype)


def get_all_perms(obj):
    """Return a list of all permissions on ``obj``."""
    return list(zip(*obj._meta.permissions))[0]


def change_perm_ctype(perm, dest_obj):
    """Keep permission action and change content type to ``dest_obj``."""
    action = get_perm_action(perm)
    return get_full_perm(action, dest_obj)


def copy_permissions(src_obj, dest_obj):
    """Copy permissions form ``src_obj`` to ``dest_obj``."""

    def _process_permission(codename, user_or_group, dest_obj, relabel):
        """Process single permission."""
        if relabel:
            codename = change_perm_ctype(codename, dest_obj)
            if codename not in dest_all_perms:
                return  # dest object doesn't have matching permission

        assign_perm(codename, user_or_group, dest_obj)

    if src_obj is None:
        return

    src_obj_ctype = ContentType.objects.get_for_model(src_obj)
    dest_obj_ctype = ContentType.objects.get_for_model(dest_obj)
    dest_all_perms = get_all_perms(dest_obj)

    relabel = src_obj_ctype != dest_obj_ctype

    for perm in UserObjectPermission.objects.filter(
        object_pk=src_obj.pk, content_type=src_obj_ctype
    ):
        _process_permission(perm.permission.codename, perm.user, dest_obj, relabel)

    for perm in GroupObjectPermission.objects.filter(
        object_pk=src_obj.pk, content_type=src_obj_ctype
    ):
        _process_permission(perm.permission.codename, perm.group, dest_obj, relabel)


def fetch_user(query):
    """Get user by ``pk``, ``username`` or ``email``.

    Raise error if user can not be determined.
    """
    lookup = Q(username=query) | Q(email=query)
    if query.isdigit():
        lookup = lookup | Q(pk=query)

    user_model = get_user_model()
    users = user_model.objects.filter(lookup)

    if not users:
        raise exceptions.ParseError("Unknown user: {}".format(query))
    elif len(users) >= 2:
        raise exceptions.ParseError("Cannot uniquely determine user: {}".format(query))
    return users[0]


def fetch_group(query):
    """Get group by ``pk`` or ``name``. Raise error if it doesn't exist."""
    group_filter = {"pk": query} if query.isdigit() else {"name": query}

    try:
        return Group.objects.get(**group_filter)
    except Group.DoesNotExist:
        raise exceptions.ParseError("Unknown group: {}".format(query))


def check_owner_permission(payload, allow_user_owner):
    """Raise ``PermissionDenied``if ``owner`` found in ``data``."""
    for entity_type in ["users", "groups"]:
        for perm_type in ["add", "remove"]:
            for perms in payload.get(entity_type, {}).get(perm_type, {}).values():
                if "owner" in perms:
                    if entity_type == "users" and allow_user_owner:
                        continue

                    if entity_type == "groups":
                        raise exceptions.ParseError(
                            "Owner permission cannot be assigned to a group"
                        )

                    raise exceptions.PermissionDenied(
                        "Only owners can grant/revoke owner permission"
                    )


def check_public_permissions(payload):
    """Raise ``PermissionDenied`` if public permissions are too open."""
    allowed_public_permissions = ["view", "add", "download"]
    for perm_type in ["add", "remove"]:
        for perm in payload.get("public", {}).get(perm_type, []):
            if perm not in allowed_public_permissions:
                raise exceptions.PermissionDenied(
                    "Permissions for public users are too open"
                )


def check_user_permissions(payload, user_pk):
    """Raise ``PermissionDenied`` if ``payload`` includes ``user_pk``."""
    for perm_type in ["add", "remove"]:
        user_pks = payload.get("users", {}).get(perm_type, {}).keys()
        if user_pk in user_pks:
            raise exceptions.PermissionDenied("You cannot change your own permissions")


def update_permission(obj, data):
    """Update object permissions."""
    full_permissions = get_all_perms(obj)

    def apply_perm(perm_func, perms, entity):
        """Apply permissions using given ``perm_func``.

        ``perm_func`` is intended to be ``assign_perms`` or
        ``remove_perms`` shortcut function from ``django-guardian``, but
        can be any function that accepts permission codename,
        user/group and object parameters (in this order).

        If given permission does not exist, ``exceptions.ParseError`` is
        raised.

        "ALL" passed as ``perms`` parameter, will call ``perm_function``
        with ``full_permissions`` list.

        :param func perm_func: Permissions function to be applied
        :param list params: list of params to be allpied
        :param entity: user or group to be passed to ``perm_func``
        :type entity: `~django.contrib.auth.models.User` or
            `~django.contrib.auth.models.Group`

        """
        if perms == "ALL":
            perms = full_permissions
        for perm in perms:
            perm_codename = get_full_perm(perm, obj)
            if perm_codename not in full_permissions:
                raise exceptions.ParseError("Unknown permission: {}".format(perm))
            perm_func(perm_codename, entity, obj)

    def set_permissions(entity_type, perm_type):
        """Set object permissions."""
        perm_func = assign_perm if perm_type == "add" else remove_perm
        fetch_fn = fetch_user if entity_type == "users" else fetch_group

        for entity_id in data.get(entity_type, {}).get(perm_type, []):
            entity = fetch_fn(entity_id)
            if entity:
                perms = data[entity_type][perm_type][entity_id]
                apply_perm(perm_func, perms, entity)

    def set_public_permissions(perm_type):
        """Set public permissions."""
        perm_func = assign_perm if perm_type == "add" else remove_perm
        user = AnonymousUser()
        perms = data.get("public", {}).get(perm_type, [])
        apply_perm(perm_func, perms, user)

    with transaction.atomic():
        set_permissions("users", "add")
        set_permissions("users", "remove")
        set_permissions("groups", "add")
        set_permissions("groups", "remove")
        set_public_permissions("add")
        set_public_permissions("remove")


def assign_contributor_permissions(obj, contributor=None):
    """Assign all permissions to object's contributor."""
    for permission in get_all_perms(obj):
        assign_perm(permission, contributor if contributor else obj.contributor, obj)
