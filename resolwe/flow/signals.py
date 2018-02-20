""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
from django.conf import settings
from django.db import transaction
from django.db.models import Count
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Entity


def commit_signal(data_id):
    """Nudge manager at the end of every Data object save event."""
    if not getattr(settings, 'FLOW_MANAGER_DISABLE_AUTO_CALLS', False):
        immediate = getattr(settings, 'FLOW_MANAGER_SYNC_AUTO_CALLS', False)
        manager.communicate(data_id=data_id, save_settings=False, run_sync=immediate)


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance, created, **kwargs):
    """Run newly created (spawned) processes."""
    if instance.status == Data.STATUS_DONE or instance.status == Data.STATUS_ERROR or created:
        # Run manager at the end of the potential transaction. Otherwise
        # tasks are send to workers before transaction ends and therefore
        # workers cannot access objects created inside transaction.
        transaction.on_commit(lambda: commit_signal(instance.id))


@receiver(pre_delete, sender=Data)
def delete_entity(sender, instance, **kwargs):
    """Delete Entity when last Data object is deleted."""
    # 1 means that the last Data object is going to be deleted.
    Entity.objects.annotate(num_data=Count('data')).filter(data=instance, num_data=1).delete()
