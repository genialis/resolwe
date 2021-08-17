""".. Ignore pydocstyle D400.

===============
Signal Handlers
===============

"""
from datetime import datetime
from typing import List, Optional, Tuple

from psycopg2.extras import DateTimeTZRange

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver
from django.utils.timezone import now

from resolwe.billing.models import ComputeCost, DataHistory, StorageTypeHistory
from resolwe.flow.models import Data
from resolwe.test.utils import is_testing


def _connect_storage_cost(
    data_history: DataHistory, current_time: Optional[datetime] = None
):
    """Create StorageTypeHistory object and connect it with DataHistory."""
    default_storage_type = "s3_standard"
    StorageTypeHistory.objects.create(
        data_history=data_history,
        storage_type=default_storage_type,
        interval=DateTimeTZRange(lower=current_time or now()),
    )


def _get_allocated_node_resources(
    requested_cpu: int, requested_memory: int, when: datetime
) -> Tuple[int, int]:
    """Get the number of CPUs and memory on the allocated node.

    Currently this is more or less an educated guess: node must have at least
    as many CPUs and memory and requested. So find first price entry that
    satisfied these criteria and return it.

    :raises RuntimeError: when no such object could be found.
    """
    cost = ComputeCost.objects.filter(
        valid__contains=when, cpu__gte=requested_cpu, memory__gte=requested_memory
    ).first()

    # TODO: is it wise to raise an error here? Just failing to enter the
    # compute cost in the database could stop the entire processing machinery.
    if cost is None:
        raise RuntimeError(
            (
                f"No cost is available for node with at least {requested_cpu} "
                "CPUs and {requested_memory} GB RAM."
            )
        )
    else:
        return (cost.cpu, cost.memory)


def create_data_history(data: Data, starting_from: datetime, first: bool):
    """Create DataHistory object for given Data object."""
    alive = DateTimeTZRange(lower=starting_from)

    # Determine the billing account in the following order:
    # - if data object is part of the collection and the collection is
    #   assigned to a billing account use it.
    # - if data contributor account has a default billing account use it.
    # - raise RuntimeError if is_testing is not set and do not raise exception
    #   if is_testing is set.

    billing_account = None
    collection = data.collection
    user = data.contributor
    if collection is not None and hasattr(collection, "billing_collection"):
        billing_account = collection.billing_collection.billing_account
    elif hasattr(user, "default_user_billing"):
        billing_account = user.default_user_billing.billing_account
    elif not is_testing:
        raise RuntimeError(f"No billing account found for Data object {data.pk}.")
    else:
        return

    processing_interval = None
    if data.started is not None and data.finished is None:
        processing_interval = DateTimeTZRange(
            lower=data.started if first else starting_from
        )
    elif data.finished is not None:
        processing_interval = DateTimeTZRange(
            lower=data.started if first else starting_from, upper=data.finished
        )

    node_cpu, node_memory = _get_allocated_node_resources(
        data.process_cores, data.process_memory, starting_from
    )

    data_history = DataHistory.objects.create(
        data=data,
        alive=alive,
        processing=processing_interval,
        collection=collection,
        cpu=data.process_cores,
        memory=data.process_memory,
        node_cpu=node_cpu,
        node_memory=node_memory,
        referenced_data_id=data.id,
        billing_account=billing_account,
        size=data.size,
        referenced_collection_id=collection.pk if collection else None,
        referenced_collection_slug=collection.slug if collection else None,
        referenced_collection_name=collection.name if collection else None,
    )
    _connect_storage_cost(data_history, starting_from)


def update_data_history(data, update_fields: Optional[List[str]]):
    """Update DataHistory object."""

    def update_collection(
        data: Data, data_history: DataHistory, current_time: datetime
    ):

        data_history.alive = DateTimeTZRange(
            lower=data_history.alive.lower, upper=current_time
        )

        if data.started is not None and data.finished is None:
            data_history.processing = DateTimeTZRange(
                lower=data_history.processing.lower, upper=current_time
            )
        data_history.save(update_fields=["alive", "processing"])
        create_data_history(data, current_time, False)

    current_time = now()
    data_history = data.data_history.order_by("id").last()

    modified_fields = []
    if update_fields is not None:
        if "collection" in update_fields or "collection_id" in update_fields:
            update_collection(data, data_history, current_time)
            return

        if "started" in update_fields:
            data.processing = DateTimeTZRange(lower=data.started)
            modified_fields.append("processing")

        if "finished" in update_fields:
            data_history.processing = DateTimeTZRange(
                lower=data.started, upper=data.finished
            )
            _connect_storage_cost(data_history, current_time)
            modified_fields.append("processing")

        if "size" in update_fields:
            data_history.size = data.size
            modified_fields.append("size")
        data_history.save(update_fields=modified_fields)

    else:
        # Collection has changed.
        # 1. Old DataHistory must be "closed".
        # 2. New DataHistory must be created with new collection.
        if data.collection != data_history.collection:
            update_collection(data, data_history, current_time)
            return

        modified = False
        if data.started is not None and data_history.processing is None:
            modified = True
            data_history.processing = DateTimeTZRange(lower=data.started)

        if data.finished is not None and data_history.processing.upper is None:
            modified = True
            data_history.processing = DateTimeTZRange(
                lower=data.started, upper=data.finished
            )
            _connect_storage_cost(data_history, current_time)

        if data.size != data_history.size:
            modified = True
            data_history.size = data.size

        if modified:
            data_history.save()


@receiver(post_save, sender=Data)
def data_post_save_handler(sender, instance: Data, created, **kwargs):
    """Run newly created (spawned) processes."""
    if created:
        transaction.on_commit(
            lambda: create_data_history(instance, instance.created, True)
        )
    else:
        transaction.on_commit(
            lambda: update_data_history(instance, kwargs.get("update_fields"))
        )


# Delete handler.
@receiver(post_delete, sender=Data)
def delete_data(sender, instance, **kwargs):
    """Delete the Relation object when the last Entity is removed."""
    data = instance
    data_history = (
        DataHistory.objects.filter(referenced_data_id=data.pk).order_by("id").last()
    )
    current_time = now()

    def delete(data_history: DataHistory):
        """Set the fields on DataHistory."""
        data_history.alive = DateTimeTZRange(
            lower=data_history.alive.lower, upper=current_time
        )
        data_history.deleted = current_time
        if (
            data_history.processing is not None
            and data_history.processing.upper is None
        ):
            data_history.processing = DateTimeTZRange(
                lower=data_history.processing.lower, upper=current_time
            )
        data_history.save()

        # Update last StorageCost and set the current time as upper interval.
        storage_type_history = (
            StorageTypeHistory.objects.filter(data_history=data_history)
            .order_by("interval")
            .last()
        )
        storage_type_history.interval = DateTimeTZRange(
            lower=storage_type_history.interval.lower, upper=current_time
        )
        storage_type_history.save()

    transaction.on_commit(lambda: delete(data_history))
