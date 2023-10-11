"""ORM signal handlers."""

from typing import Union

from django import dispatch
from django.db.models import Model
from django.db.models import signals as model_signals

from resolwe.flow.models import Collection, Data, Entity
from resolwe.flow.signals import post_duplicate
from resolwe.permissions.models import Permission

from .models import Observable, Observer
from .protocol import (
    ChangeType,
    post_container_changed,
    post_permission_changed,
    pre_container_changed,
    pre_permission_changed,
    suppress_notifications_attribute,
)

# Global 'in migrations' flag to ignore signals during migrations.
# Signal handlers that access the database can crash the migration process.
IN_MIGRATIONS = False

Duplicate = Union[Data, Entity, Collection]


@dispatch.receiver(model_signals.pre_migrate)
def model_pre_migrate(*args, **kwargs):
    """Set 'in migrations' flag."""
    global IN_MIGRATIONS
    IN_MIGRATIONS = True


@dispatch.receiver(model_signals.post_migrate)
def model_post_migrate(*args, **kwargs):
    """Clear 'in migrations' flag."""
    global IN_MIGRATIONS
    IN_MIGRATIONS = False


def skip_in_migrations(function):
    """Execute the given method if not in migrations."""

    def wrapped(*args, **kwargs):
        if not IN_MIGRATIONS:
            function(*args, **kwargs)

    return wrapped


@dispatch.receiver(pre_permission_changed)
@skip_in_migrations
def prepare_permission_change(instance, **kwargs):
    """Store old permissions for an object whose permissions are about to change."""
    instance._old_viewers = instance.users_with_permission(
        Permission.VIEW, with_superusers=True
    )


@dispatch.receiver(pre_container_changed)
@skip_in_migrations
def prepare_container_change(instance, **kwargs):
    """Store old containerds for an object."""
    prepare_permission_change(instance)
    instance._old_containers = set(instance.containers)


@dispatch.receiver(post_container_changed)
@skip_in_migrations
def handle_container_change(instance, **kwargs):
    """Send notifications when object is moved to another container."""
    old_containers = getattr(instance, "_old_containers", set())
    new_containers = set(instance.containers)
    removed_from = old_containers - new_containers
    added_to = new_containers - old_containers

    # Handle possible permission change (CREATE/DELETE) on the object itself. Do
    # not send notifications on the containers, this is handled bellow.
    handle_permission_change(instance, observe_containers=False)
    # Handle notifications (CREATE/DELETE) on the containers.
    Observer.observe_instance_container(instance, ChangeType.DELETE, removed_from)
    Observer.observe_instance_container(instance, ChangeType.CREATE, added_to)
    # Handle UPDATE notifications on the instance (container property has changed).
    Observer.observe_instance_changes(instance, ChangeType.UPDATE)


@dispatch.receiver(post_permission_changed)
@skip_in_migrations
def handle_permission_change(instance, **kwargs):
    """Compare permissions for an object whose permissions changed."""
    new = set(instance.users_with_permission(Permission.VIEW, with_superusers=True))
    # The "_old_viewers" property may not exist. For instance if data object is
    # created in the collection and collection permissions are assigned to it
    # without set_permission call.
    old = set(getattr(instance, "_old_viewers", []))
    gains = new - old
    losses = old - new
    observe_containers = kwargs.get("observe_containers", True)
    Observer.observe_permission_changes(instance, gains, losses, observe_containers)


@dispatch.receiver(model_signals.post_save)
@skip_in_migrations
def observe_model_modification(
    sender: type, instance: Model, created: bool = False, **kwargs
):
    """Receive model updates.

    We have to consider the following options.
    1. The object was created: send CREATE notifications to users with VIEW permission.
    2. The object was changed: send UPDATE notifications to users with VIEW permission.

    Do not send notifications to the containers when object is moving between them.
    """
    if isinstance(instance, Observable):
        if created:
            handle_permission_change(instance)
        elif not getattr(instance, suppress_notifications_attribute, False):
            Observer.observe_instance_changes(instance, ChangeType.UPDATE)
            Observer.observe_instance_container(instance, ChangeType.UPDATE)


@dispatch.receiver(model_signals.pre_delete)
@skip_in_migrations
def observe_model_deletion(sender: type, instance: Model, **kwargs):
    """Receive model deletions."""
    if isinstance(instance, Observable):
        Observer.observe_instance_changes(instance, ChangeType.DELETE)
        Observer.observe_instance_container(instance, ChangeType.DELETE)


@dispatch.receiver(post_duplicate)
def post_duplicate_models(
    sender, instances: list[Duplicate], old_instances: list[Duplicate], **kwargs
):
    """Send create notification when duplicates are created.

    This is necessary since bulk_duplicate does not trigger post_save signals.
    """
    # Make sure all instances have the same permisions.
    gains = dict()
    for instance in instances:
        if instance.permission_group_id not in gains:
            gains[instance.permission_group_id] = set(
                instance.users_with_permission(Permission.VIEW, with_superusers=True)
            )

    for instance in instances:
        Observer.observe_permission_changes(
            instance, gains[instance.permission_group_id], set()
        )
