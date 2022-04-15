""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
from typing import List, Optional

from asgiref.sync import async_to_sync

from django.conf import settings
from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import Signal, receiver

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Relation
from resolwe.flow.models.entity import RelationPartition

# The signal sent before processing of the Data object starts. It is useful to
# attach to it if some other models etc have to be created before the actual
# processing takes place.
before_processing = Signal()

# Sent when data objects are copied in bulk (for instance when duplicating
# data objects) and no other signals are sent.
post_duplicate = Signal()


def commit_signal(data: Data, created: bool, update_fields: Optional[List[str]]):
    """Nudge manager at the end of every Data object save event.

    Also sent before_processing signal just before nudging manager.
    """
    if not getattr(settings, "FLOW_MANAGER_DISABLE_AUTO_CALLS", False):
        before_processing.send(
            sender=Data, instance=data, created=created, update_fields=update_fields
        )
        immediate = getattr(settings, "FLOW_MANAGER_SYNC_AUTO_CALLS", False)
        async_to_sync(manager.communicate)(data_id=data.id, run_sync=immediate)


@receiver(post_save, sender=Data)
def manager_post_save_handler(sender, instance: Data, created: bool, **kwargs):
    """Run newly created (spawned) processes."""
    if (
        instance.status == Data.STATUS_DONE
        or instance.status == Data.STATUS_ERROR
        or created
    ):
        # Run manager at the end of the potential transaction. Otherwise
        # tasks are send to workers before transaction ends and therefore
        # workers cannot access objects created inside transaction.
        transaction.on_commit(
            lambda: commit_signal(instance, created, kwargs.get("update_fields"))
        )


# NOTE: m2m_changed signal cannot be used because of a bug:
# https://code.djangoproject.com/ticket/17688
@receiver(post_delete, sender=RelationPartition)
def delete_relation(sender, instance: RelationPartition, **kwargs):
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
