"""Resolwe history model.

It is used to track changes in objects.
"""

from typing import Any, Iterable, Type

from django.contrib.postgres.fields import DateTimeRangeField
from django.db import models
from django.db.backends.postgresql.psycopg_any import DateTimeTZRange

from .base import MAX_NAME_LENGTH, MAX_SLUG_LENGTH
from .collection import Collection
from .data import Data
from .history_manager import HistoryMixin, history_manager


class BaseHistory(models.Model):
    """BaseHistory object."""

    class Meta:
        """Make this model abstract."""

        abstract = True

    #: When this object is valid for
    valid = DateTimeRangeField()

    #: When the object was deleted
    deleted = models.DateTimeField(blank=True, null=True)


class DataHistory(BaseHistory):
    """Track Data history."""

    #: Pointer to the data object, can be null
    datum = models.ForeignKey(
        Data, on_delete=models.SET_NULL, related_name="history", null=True
    )


class CollectionHistory(BaseHistory):
    """Track Collection history."""

    #: Pointer to the collection object, can be null
    datum = models.ForeignKey(
        Collection, on_delete=models.SET_NULL, related_name="history", null=True
    )


class TrackChange(models.Model):
    """Base class to track the changes in the model."""

    class Meta:
        """TrackChange meta options."""

        abstract = True
        ordering = ["timestamp"]
        indexes = [
            models.Index(name="idx_%(class)s_time", fields=["timestamp"]),
        ]

    # Override in the child class.
    # The itarable of tuple (model, field_name) this tracker can be applied to.
    applicable_to: Iterable[tuple[Type[models.Model], str]]

    def __init_subclass__(cls: Type["TrackChange"], **kwargs):
        """Register instance of the class as plugin."""
        super().__init_subclass__(**kwargs)
        history_manager.add_tracker(cls)

    #: Defined to keep mypy from reporting errors (objects do not exist since class is
    #: abstract).
    objects: models.Manager

    #: Timestamp of the change
    timestamp = models.DateTimeField(auto_now_add=True)

    # Here for mypy to detect all objects of type TrackChange must have value.
    value: Any

    def get_value(self, field_name: str) -> Any:
        """Return the value used to track changes."""
        return self.value

    @staticmethod
    def preprocess_value(instance: models.Model, field_name: str) -> Any:
        """Preprocess the value before saving."""
        return getattr(instance, field_name)

    @classmethod
    def process_change(cls, instance: HistoryMixin, field_name: str):
        """Create new track change object."""
        value = cls.preprocess_value(instance, field_name)
        cls.objects.create(history=instance.get_history_object(), value=value)


class DataSlugChange(TrackChange):
    """Track data slug changes."""

    applicable_to = ((Data, "slug"),)

    #: New value.
    value = models.CharField(max_length=MAX_SLUG_LENGTH)

    #: Reference to the history object
    history = models.ForeignKey(
        DataHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )


class CollectionSlugChange(TrackChange):
    """Track collection slug changes."""

    applicable_to = ((Collection, "slug"),)

    #: New value.
    value = models.CharField(max_length=MAX_SLUG_LENGTH)

    #: Reference to the history object
    history = models.ForeignKey(
        CollectionHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )


class DataNameChange(TrackChange):
    """Track data name changes."""

    applicable_to = ((Data, "name"),)

    #: New value.
    value = models.CharField(max_length=MAX_NAME_LENGTH)

    #: Reference to the history object
    history = models.ForeignKey(
        DataHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )


class CollectionNameChange(TrackChange):
    """Track collection name changes."""

    applicable_to = ((Collection, "name"),)

    #: New value.
    value = models.CharField(max_length=MAX_NAME_LENGTH)

    #: Reference to the history object
    history = models.ForeignKey(
        CollectionHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )


class CollectionChange(TrackChange):
    """Track collection changes."""

    applicable_to = ((Data, "collection"),)

    #: New value.
    value = models.ForeignKey(
        CollectionHistory,
        on_delete=models.PROTECT,
        related_name="collection_changes",
        null=True,
    )

    #: Reference to the history object
    history = models.ForeignKey(
        DataHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )

    @staticmethod
    def preprocess_value(instance: models.Model, field_name: str) -> Any:
        """Preprocess the value before saving."""
        value = getattr(instance, field_name)
        if isinstance(value, Collection):
            return value.get_history_object()
        return value


class SizeChange(TrackChange):
    """Track size changes."""

    applicable_to = ((Data, "size"),)

    #: New value
    value = models.BigIntegerField()

    #: Reference to the history object
    history = models.ForeignKey(
        DataHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )


class ProcessingHistory(TrackChange):
    """Track processing changes."""

    applicable_to = ((Data, "finished"),)

    #: Reference to the history object
    history = models.ForeignKey(
        BaseHistory, on_delete=models.PROTECT, related_name="processing"
    )

    #: Processing interval
    interval = DateTimeRangeField()

    #: Number of CPU allocated to the process
    allocated_cpu = models.IntegerField()

    #: Amount of memory (in bytes) allocated to the process
    allocated_memory = models.BigIntegerField()

    #: Number of CPU on machine running the process
    node_cpu = models.IntegerField()

    #: Amount of memory on machine running the process
    node_memory = models.BigIntegerField()

    #: Reference to the history object
    history = models.ForeignKey(
        DataHistory, on_delete=models.PROTECT, related_name="%(class)s"
    )

    def get_value(self, field_name: str) -> Any:
        """Return the value used to track changes."""
        return self.interval.upper

    @classmethod
    def process_change(cls, instance: HistoryMixin, field_name: str):
        """Preprocess the value before saving."""
        assert isinstance(instance, Data), "The instance must be Data instance."
        assert field_name == "finished", "The field name must be 'finished'."
        started = instance.started
        finished = instance.finished
        history = instance.get_history_object()

        if finished is not None:
            return ProcessingHistory.objects.create(
                interval=DateTimeTZRange(lower=started, upper=finished),
                history=history,
                allocated_cpu=instance.process_cores,
                allocated_memory=instance.process_memory,
                node_cpu=instance.process_cores,
                node_memory=instance.process_memory,
            )
        return cls.objects.last()
