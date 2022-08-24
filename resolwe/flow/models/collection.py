"""Resolwe collection model."""
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.db import models, transaction

from resolwe.permissions.models import PermissionObject, PermissionQuerySet

from .base import BaseModel, BaseQuerySet
from .utils import DirtyError, bulk_duplicate, validate_schema


class BaseCollection(BaseModel):
    """Template for Postgres model for storing a collection."""

    class Meta(BaseModel.Meta):
        """BaseCollection Meta options."""

        abstract = True

    #: detailed description
    description = models.TextField(blank=True)

    settings = models.JSONField(default=dict)

    #: collection descriptor schema
    descriptor_schema = models.ForeignKey(
        "flow.DescriptorSchema", blank=True, null=True, on_delete=models.PROTECT
    )

    #: collection descriptor
    descriptor = models.JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)

    #: field used for full-text search
    search = SearchVectorField(null=True)

    def save(self, *args, **kwargs):
        """Perform descriptor validation and save object."""
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
        super().save()


class CollectionQuerySet(BaseQuerySet, PermissionQuerySet):
    """Query set for ``Collection`` objects."""

    @transaction.atomic
    def duplicate(self, contributor):
        """Duplicate (make a copy) ``Collection`` objects."""
        return bulk_duplicate(collections=self, contributor=contributor)


class Collection(BaseCollection, PermissionObject):
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
        "AnnotationField", related_name="collection"
    )

    def is_duplicate(self):
        """Return True if collection is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, contributor):
        """Duplicate (make a copy)."""
        return bulk_duplicate(
            collections=self._meta.model.objects.filter(pk=self.pk),
            contributor=contributor,
        )[0]
