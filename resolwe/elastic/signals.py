""".. Ignore pydocstyle D400.

=======================
Elastic Signal Handlers
=======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db.models.signals import pre_delete, post_save
from django.dispatch import receiver

from guardian.models import GroupObjectPermission, UserObjectPermission

from .builder import index_builder


def _process_permission(perm):
    """Rebuild indexes affected by the given permission."""
    # XXX: Optimize: rebuild only permissions, not whole document
    if not perm.permission.codename.startswith('view'):
        return

    obj = perm.content_type.get_object_for_this_type(id=perm.object_pk)
    index_builder.build(obj)


@receiver(post_save, sender=UserObjectPermission)
def add_user_permission(sender, instance, **kwargs):
    """Process indexes after adding user permission."""
    _process_permission(instance)


@receiver(post_save, sender=GroupObjectPermission)
def add_group_permission(sender, instance, **kwargs):
    """Process indexes after adding group permission."""
    _process_permission(instance)


@receiver(pre_delete, sender=UserObjectPermission)
def remove_user_permission(sender, instance, **kwargs):
    """Process indexes after removing user permission."""
    _process_permission(instance)


@receiver(pre_delete, sender=GroupObjectPermission)
def remove_group_permission(sender, instance, **kwargs):
    """Process indexes after removing group permission."""
    _process_permission(instance)
