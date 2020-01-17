""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
from asgiref.sync import async_to_sync

from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Relation
from resolwe.flow.models.entity import RelationPartition


def commit_signal(data_id):
    """Nudge manager at the end of every Data object save event."""
    if not getattr(settings, "FLOW_MANAGER_DISABLE_AUTO_CALLS", False):
        immediate = getattr(settings, "FLOW_MANAGER_SYNC_AUTO_CALLS", False)
        async_to_sync(manager.communicate)(
            data_id=data_id, save_settings=False, run_sync=immediate
        )


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance, created, **kwargs):
    """Run newly created (spawned) processes."""
    if (
        instance.status == Data.STATUS_DONE
        or instance.status == Data.STATUS_ERROR
        or created
    ):
        # Run manager at the end of the potential transaction. Otherwise
        # tasks are send to workers before transaction ends and therefore
        # workers cannot access objects created inside transaction.
        transaction.on_commit(lambda: commit_signal(instance.id))


# NOTE: m2m_changed signal cannot be used because of a bug:
# https://code.djangoproject.com/ticket/17688
@receiver(post_delete, sender=RelationPartition)
def delete_relation(sender, instance, **kwargs):
    """Delete the Relation object when the last Entity is removed."""

    def process_signal(relation_id):
        """Get the relation and delete it if it has no entities left."""
        try:
            relation = Relation.objects.get(pk=relation_id)
        except Relation.DoesNotExist:
            return

        if relation.entities.count() == 0:
            relation.delete()

    # Wait for partitions to be recreated.
    transaction.on_commit(lambda: process_signal(instance.relation_id))
