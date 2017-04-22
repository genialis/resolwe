""".. Ignore pydocstyle D400.

=======================
Elastic Signal Handlers
=======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver

from guardian.models import GroupObjectPermission, UserObjectPermission

from .builder import index_builder
from .utils import prepare_connection

try:
    import celery
except ImportError:
    celery = None


def _process_permission(perm):
    """Rebuild indexes affected by the given permission."""
    # XXX: Optimize: rebuild only permissions, not whole document
    if not perm.permission.codename.startswith('view'):
        return

    index_builder.build(perm.content_object)


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


if celery is not None:
    @receiver(celery.signals.worker_process_init)
    def refresh_connection(sender, **kwargs):
        """Refresh connection to Elasticsearch when worker is started.

        File descriptors (sockets) can be shared between multiple
        processes. If same connection is used by multiple processes at
        the same time, this can cause timeouts in some of the tasks.
        So connection needs to be reestablished after worker is started
        to make sure that it is unique per worker.
        """
        prepare_connection()
