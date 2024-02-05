"""History manager.

TODO: move it to a 1different namespace.
"""

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, Type

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.db.backends.postgresql.psycopg_any import DateTimeTZRange
from django.utils import timezone

if TYPE_CHECKING:
    from .history import TrackChange

    _Base = models.Model
else:
    _Base = object


class HistoryManager:
    """Keeps track of the history changes.

    All tracked fields are registered automatically and changes must be tracked through
    this model.
    """

    def __init__(self):
        """Initialize mapping between field name and tracker."""
        self._trackers: dict[tuple[Type[models.Model], str], Type["TrackChange"]] = (
            dict()
        )
        self._tracked_fields: dict[Type[models.Model], list[str]] = defaultdict(list)

    def add_tracker(self, tracker: Type["TrackChange"]):
        """Add tracker to the manager.

        :raises AssertionError: when tracker for the same field already exists.
        """
        for model, name in tracker.applicable_to:
            assert (
                model,
                name,
            ) not in self._trackers, (
                f"Tracker for field '{name}' and model '{model}' already exists."
            )
            self._trackers[(model, name)] = tracker
            self._tracked_fields[model].append(name)

    def valid_value(
        self,
        instance: "HistoryMixin",
        field_name: str,
        timestamp: Optional[datetime] = None,
    ) -> Any:
        """Get the valid value at the point in time.

        :attr timestamp: a point in time. When not provided the current moment is used.
        :raises AttributeError: when no value exists for the given timestamp.
        """
        # Take the latest change before the timestamp. When timestamp is not provided
        # take the latest change.
        history = instance.get_history_object()
        tracker = self._trackers[(instance._meta.model, field_name)]
        if timestamp:
            tracker_qset = tracker.objects.filter(
                history=history, timestamp__lte=timestamp
            )
        else:
            tracker_qset = tracker.objects.filter(history=history)

        change = tracker_qset.last()
        if change is None:
            raise RuntimeError(
                f"No value exists for history object {history.pk}, field "
                f"{tracker.applicable_to} and timestamp {timestamp}."
            )
        return change.get_value(field_name)

    def process_change(self, instance: "HistoryMixin"):
        """Process the change in the field.

        :raises KeyError: when tracker for the field is not registered.
        :raises AssertionError: when the instance does not have history tracking.
        """
        model = instance._meta.model
        for field_name in self._tracked_fields[model]:
            if (model, field_name) not in self._trackers:
                continue
            try:
                tracker = self._trackers[(model, field_name)]
                if hasattr(instance, field_name):
                    current_value = self.valid_value(instance, field_name)
                    new_value = getattr(instance, field_name)
                    if current_value != new_value:
                        tracker.process_change(instance, field_name)
            except RuntimeError:
                tracker.process_change(instance, field_name)


history_manager = HistoryManager()


class HistoryMixin(_Base):
    """Extend this model to track its changes."""

    # For typing purposes.
    history: models.Manager

    def delete(self, *args, **kwargs):
        """Delete the object and update the history object."""
        history_object = self.get_history_object()
        super().delete(*args, **kwargs)
        history_object._meta.model.objects.filter(pk=history_object.pk).update(
            deleted=timezone.now()
        )

    def save(self, *args, **kwargs):
        """Save the object and update the history object."""
        super().save(*args, **kwargs)
        history_manager.process_change(self)

    def get_history_object(self):
        """Get the valid history object for the instance.

        If the object does not exist yet, create it.
        """
        try:
            return self.history.get(valid__upper_inf=True)
        except ObjectDoesNotExist:
            return self.history.create(valid=DateTimeTZRange(lower=timezone.now()))

    def get_history_model(self):
        """Get the model of the history object."""
        return self.history.model
