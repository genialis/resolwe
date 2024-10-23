"""Resolwe collection model."""

from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.db import models

from resolwe.flow.models.base import BaseQuerySet
from resolwe.observers.consumers import BackgroundTaskType
from resolwe.observers.models import BackgroundTask
from resolwe.observers.utils import start_background_task
from resolwe.permissions.models import PermissionObject

from .annotations import AnnotationField
from .base import BaseModel
from .history_manager import HistoryMixin
from .utils import DirtyError, validate_schema


class BaseCollection(BaseModel):
    """Template for Postgres model for storing a collection."""

    class Meta(BaseModel.Meta):
        """BaseCollection Meta options."""

        abstract = True

    #: detailed description
    description = models.TextField(blank=True)

    settings = models.JSONField(default=dict)

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)

    #: field used for full-text search
    search = SearchVectorField(null=True)


class CollectionQuerySet(BaseQuerySet):
    """Query set for ``Collection`` objects."""

    def duplicate(self, request_user) -> BackgroundTask:
        """Duplicate (make a copy) ``Collection`` objects in the background."""
        task_data = {"collection_ids": list(self.values_list("pk", flat=True))}
        return start_background_task(
            BackgroundTaskType.DUPLICATE_COLLECTION,
            "Duplicate collections",
            task_data,
            request_user,
        )

    def delete_background(self, request_user):
        """Delete the ``Collection`` objects in the background."""
        task_data = {
            "object_ids": list(self.values_list("pk", flat=True)),
            "content_type_id": ContentType.objects.get_for_model(self.model).pk,
        }
        return start_background_task(
            BackgroundTaskType.DELETE, "Delete collections", task_data, request_user
        )


class Collection(HistoryMixin, PermissionObject, BaseCollection):
    """Postgres model for storing a collection."""

    class Meta(BaseCollection.Meta):
        """Collection Meta options."""

        permissions = (
            ("view", "Can view collection"),
            ("edit", "Can edit collection"),
            ("share", "Can share collection"),
            ("owner", "Is owner of the collection"),
        )

        indexes = [
            models.Index(name="idx_collection_name", fields=["name"]),
            GinIndex(
                name="idx_collection_name_trgm",
                fields=["name"],
                opclasses=["gin_trgm_ops"],
            ),
            models.Index(name="idx_collection_slug", fields=["slug"]),
            GinIndex(name="idx_collection_tags", fields=["tags"]),
            GinIndex(name="idx_collection_search", fields=["search"]),
        ]

    #: manager
    objects = CollectionQuerySet.as_manager()

    #: duplication date and time
    duplicated = models.DateTimeField(blank=True, null=True)

    #: annotation fields available to samples in this collection
    annotation_fields = models.ManyToManyField(
        AnnotationField, related_name="collection"
    )

    #: collection descriptor schema
    descriptor_schema = models.ForeignKey(
        "flow.DescriptorSchema", blank=True, null=True, on_delete=models.PROTECT
    )

    #: collection descriptor
    descriptor = models.JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    def is_duplicate(self):
        """Return True if collection is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, request_user) -> BackgroundTask:
        """Duplicate (make a copy) object in the background."""
        task_data = {"collection_ids": [self.pk]}
        return start_background_task(
            BackgroundTaskType.DUPLICATE_COLLECTION,
            "Duplicate collection",
            task_data,
            request_user,
        )

    def delete_background(self):
        """Delete the object in the background."""
        task_data = {
            "object_ids": [self.pk],
            "content_type_id": ContentType.objects.get_for_model(self).pk,
        }
        return start_background_task(
            BackgroundTaskType.DELETE, "Delete collections", task_data, self.contributor
        )

    def save(self, *args, **kwargs):
        """Perform descriptor validation and save object."""
        create = self.pk is None
        if self.descriptor_schema:
            try:
                validate_schema(self.descriptor, self.descriptor_schema.schema)
                self.descriptor_dirty = False
            except DirtyError:
                self.descriptor_dirty = True
        elif self.descriptor and self.descriptor != {}:
            raise ValueError(
                "`descriptor_schema` must be defined if `descriptor` is given"
            )
        super().save(*args, **kwargs)
        if create:
            required_fields = AnnotationField.objects.filter(required=True)
            if required_fields.exists():
                self.annotation_fields.add(*required_fields)
