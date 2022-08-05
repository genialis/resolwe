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
