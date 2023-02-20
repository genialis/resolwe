"""Constants used for Observer communication."""
from enum import Enum, auto

from django import dispatch


class ChangeType(Enum):
    """Types of database changes."""

    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


# Group used for individual sessions.
GROUP_SESSIONS = "observers.session.{session_id}"

# Message type for observer item updates.
TYPE_ITEM_UPDATE = "observers.item_update"

# Signal to be sent before and after PermissionObject.set_permission is called
# or before and after a PermissionObject's container is changed.
pre_permission_changed = dispatch.Signal(providing_args=["instance"])
post_permission_changed = dispatch.Signal(providing_args=["instance"])

# Signal to be sent before and after container is changed.
pre_container_changed = dispatch.Signal(providing_args=["instance"])
post_container_changed = dispatch.Signal(providing_args=["instance"])

# Attribute to use on the observable instance to temporary suppress notifications
suppress_notifications_attribute = "__observer_notification_suppressed"
