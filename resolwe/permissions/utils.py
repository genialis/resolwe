""".. Ignore pydocstyle D400.

=================
Permissions utils
=================

.. autofunction:: copy_permissions

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.contrib.contenttypes.models import ContentType
from django.db import transaction

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, remove_perm
from rest_framework import exceptions


def copy_permissions(src_obj, dest_obj):
    """Copy permissions form ``src_obj`` to ``dest_obj``."""
    src_obj_ctype = ContentType.objects.get_for_model(src_obj)
    dest_obj_ctype = ContentType.objects.get_for_model(dest_obj)

    if src_obj_ctype != dest_obj_ctype:
        raise AssertionError('Content types of source and destination objects are not equal.')

    for perm in UserObjectPermission.objects.filter(object_pk=src_obj.pk, content_type=src_obj_ctype):
        assign_perm(perm.permission.codename, perm.user, dest_obj)
    for perm in GroupObjectPermission.objects.filter(object_pk=src_obj.pk, content_type=src_obj_ctype):
        assign_perm(perm.permission.codename, perm.group, dest_obj)


def fetch_user(query):
    """Get user by ``pk`` or ``username``. Raise error if it doesn't exist."""
    user_filter = {'pk': query} if query.isdigit() else {'username': query}
    user_model = get_user_model()

    try:
        return user_model.objects.get(**user_filter)
    except user_model.DoesNotExist:
        raise exceptions.ParseError("Unknown user: {}".format(query))


def fetch_group(query):
    """Get group by ``pk`` or ``name``. Raise error if it doesn't exist."""
    group_filter = {'pk': query} if query.isdigit() else {'name': query}

    try:
        return Group.objects.get(**group_filter)
    except Group.DoesNotExist:
        raise exceptions.ParseError("Unknown group: {}".format(query))


def check_owner_permission(payload, allow_user_owner):
    """Raise ``PermissionDenied``if ``owner`` found in ``data``."""
    for entity_type in ['users', 'groups']:
        for perm_type in ['add', 'remove']:
            for perms in payload.get(entity_type, {}).get(perm_type, {}).values():
                if 'owner' in perms:
                    if entity_type == 'users' and allow_user_owner:
                        continue

                    if entity_type == 'groups':
                        raise exceptions.ParseError("Owner permission cannot be assigned to a group")

                    raise exceptions.PermissionDenied("Only owners can grant/revoke owner permission")


def check_public_permissions(payload):
    """Raise ``PermissionDenied`` if public permissions are too open."""
    allowed_public_permissions = ['view', 'add', 'download']
    for perm_type in ['add', 'remove']:
        for perm in payload.get('public', {}).get(perm_type, []):
            if perm not in allowed_public_permissions:
                raise exceptions.PermissionDenied("Permissions for public users are too open")


def check_user_permissions(payload, user_pk):
    """Raise ``PermissionDenied`` if ``payload`` includes ``user_pk``."""
    for perm_type in ['add', 'remove']:
        user_pks = payload.get('users', {}).get(perm_type, {}).keys()
        if user_pk in user_pks:
            raise exceptions.PermissionDenied("You cannot change your own permissions")


def remove_permission(payload, permission):
    """Remove all occurrences of ``permission`` from ``payload``."""
    payload = copy.deepcopy(payload)

    for entity_type in ['users', 'groups']:
        for perm_type in ['add', 'remove']:
            for perms in payload.get(entity_type, {}).get(perm_type, {}).values():
                if permission in perms:
                    perms.remove(permission)

    for perm_type in ['add', 'remove']:
        perms = payload.get('public', {}).get(perm_type, [])
        if permission in perms:
            perms.remove(permission)

    return payload


def update_permission(obj, data):
    """Update object permissions."""
    content_type = ContentType.objects.get_for_model(obj)
    # Camel case class names are converted into a space-separated
    # content types, so spaces have to be removed.
    content_type = str(content_type).replace(' ', '')
    full_permissions = list(zip(*obj._meta.permissions))[0]  # pylint: disable=protected-access

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
        if perms == u'ALL':
            perms = full_permissions
        for perm in perms:
            perm_codename = '{}_{}'.format(perm.lower(), content_type)
            if perm_codename not in full_permissions:
                raise exceptions.ParseError("Unknown permission: {}".format(perm))
            perm_func(perm_codename, entity, obj)

    def set_permissions(entity_type, perm_type):
        """Set object permissions."""
        perm_func = assign_perm if perm_type == 'add' else remove_perm
        fetch_fn = fetch_user if entity_type == 'users' else fetch_group

        for entity_id in data.get(entity_type, {}).get(perm_type, []):
            entity = fetch_fn(entity_id)
            if entity:
                perms = data[entity_type][perm_type][entity_id]
                apply_perm(perm_func, perms, entity)

    def set_public_permissions(perm_type):
        """Set public permissions."""
        perm_func = assign_perm if perm_type == 'add' else remove_perm
        user = AnonymousUser()
        perms = data.get('public', {}).get(perm_type, [])
        apply_perm(perm_func, perms, user)

    with transaction.atomic():
        set_permissions('users', 'add')
        set_permissions('users', 'remove')
        set_permissions('groups', 'add')
        set_permissions('groups', 'remove')
        set_public_permissions('add')
        set_public_permissions('remove')


def assign_contributor_permissions(obj, contributor=None):
    """Assign all permissions to object's contributor."""
    for permission in list(zip(*obj._meta.permissions))[0]:  # pylint: disable=protected-access
        assign_perm(permission, contributor if contributor else obj.contributor, obj)
