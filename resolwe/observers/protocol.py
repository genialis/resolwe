"""Constants used for Observer communication."""

from enum import IntEnum
from typing import Optional, TypedDict

from django import dispatch


class ChangeType(IntEnum):
    """Types of database changes."""

    CREATE = 1
    UPDATE = 2
    DELETE = 3


# Group used for individual sessions.
GROUP_SESSIONS = "observers.session.{session_id}"

# Message type for observer item updates.
TYPE_ITEM_UPDATE = "observers.item_update"

# Signal to be sent before and after PermissionObject.set_permission is called
# or before and after a PermissionObject's container is changed.
pre_permission_changed = dispatch.Signal()
post_permission_changed = dispatch.Signal()

# Signal to be sent before and after container is changed.
pre_container_changed = dispatch.Signal()
post_container_changed = dispatch.Signal()

# Attribute to use on the observable instance to temporary suppress notifications
suppress_notifications_attribute = "__observer_notification_suppressed"


class ChannelsMessage(TypedDict):
    """The type for channels message to be sent."""

    type: str
    object_id: int
    content_type_pk: int
    change_type_value: int
    source: Optional[tuple[str, int]]


class WebsocketMessage(TypedDict):
    """The type of websocket message to be sent.

    :attr object_id: the id of the object to notify
    :attr change_type: the type of change (create/delete/update)
    :attr subscription_id: the id of the subscription
    :attr source: the tuple (content_type, source_id) of the object, that triggered
        this notification. For instence when data object with id 1 in the collection
        with id 2 is modified, the following messages are sent.
        * {1, update, (data, 1), subscription_id }
        * {2, update, (data, 1), subscription_id }
    """

    object_id: Optional[int]
    change_type: str
    source: Optional[tuple[str, int]]
    subscription_id: str
