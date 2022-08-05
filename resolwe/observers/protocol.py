"""Constants used for Observer communication."""
from enum import Enum, auto


class ChangeType(Enum):
    """Types of database changes."""

    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


# Group used for individual sessions.
GROUP_SESSIONS = "observers.session.{session_id}"

# Message type for observer item updates.
TYPE_ITEM_UPDATE = "observers.item_update"
