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
    """Get user by ``pk`` or ``username``. Return ``None`` if doesn't exist."""
    user_filter = {'pk': query} if query.isdigit() else {'username': query}
    return get_user_model().objects.get(**user_filter)


def fetch_group(query):
    """Get group by ``pk`` or ``name``. Return ``None`` if doesn't exist."""
    group_filter = {'pk': query} if query.isdigit() else {'name': query}
    return Group.objects.get(**group_filter)


def check_owner_permission(payload):
    """Raise ``PermissionDenied``if ``owner`` found in ``data``."""
    for entity_type in ['users', 'groups']:
        for perm_type in ['add', 'remove']:
            for perms in payload.get(entity_type, {}).get(perm_type, {}).values():
                if 'owner' in perms:
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
    full_permissions = list(zip(*obj._meta.permissions))[0]  # pylint: disable=protected-access

    def set_permissions(entity_type, perm_type):
        """Set object permissions."""
        perm_func = assign_perm if perm_type == 'add' else remove_perm
        fetch_fn = fetch_user if entity_type == 'users' else fetch_group

        for entity_id in data.get(entity_type, {}).get(perm_type, []):
            entity = fetch_fn(entity_id)
            if entity:
                perms = data[entity_type][perm_type][entity_id]
                if perms == u'ALL':
                    perms = full_permissions
                for perm in perms:
                    perm_func('{}_{}'.format(perm.lower(), content_type), entity, obj)

    set_permissions('users', 'add')
    set_permissions('users', 'remove')
    set_permissions('groups', 'add')
    set_permissions('groups', 'remove')

    def set_public_permissions(perm_type):
        """Set public permissions."""
        perm_func = assign_perm if perm_type == 'add' else remove_perm
        user = AnonymousUser()
        perms = data.get('public', {}).get(perm_type, [])
        if perms == u'ALL':
            perms = full_permissions
        for perm in perms:
            perm_func('{}_{}'.format(perm.lower(), content_type), user, obj)

    set_public_permissions('add')
    set_public_permissions('remove')


def assign_contributor_permissions(obj):
    """Assign all permissions to object's contributor."""
    for permission in list(zip(*obj._meta.permissions))[0]:  # pylint: disable=protected-access
        assign_perm(permission, obj.contributor, obj)
