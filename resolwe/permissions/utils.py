""".. Ignore pydocstyle D400.

=================
Permissions utils
=================

.. autofunction:: copy_permissions

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.contenttypes.models import ContentType

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm


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
