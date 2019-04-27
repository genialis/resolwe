"""Resolwe collection model."""
from django.contrib.postgres.fields import ArrayField, JSONField
from django.db import models, transaction
from django.utils.timezone import now

from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import assign_contributor_permissions

from .base import BaseModel
from .utils import DirtyError, validate_schema


class BaseCollection(BaseModel):
    """Template for Postgres model for storing a collection."""

    class Meta(BaseModel.Meta):
        """BaseCollection Meta options."""

        abstract = True

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField(default=dict)

    data = models.ManyToManyField('flow.Data')

    #: collection descriptor schema
    descriptor_schema = models.ForeignKey('flow.DescriptorSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: collection descriptor
    descriptor = JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)

    def save(self, *args, **kwargs):
        """Perform descriptor validation and save object."""
        if self.descriptor_schema:
            try:
                validate_schema(self.descriptor, self.descriptor_schema.schema)  # pylint: disable=no-member
                self.descriptor_dirty = False
            except DirtyError:
                self.descriptor_dirty = True
        elif self.descriptor and self.descriptor != {}:
            raise ValueError("`descriptor_schema` must be defined if `descriptor` is given")

        super().save()


class CollectionQuerySet(models.QuerySet):
    """Query set for ``Collection`` objects."""

    @transaction.atomic
    def duplicate(self, contributor=None):
        """Duplicate (make a copy) ``Collection`` objects."""
        return [
            collection.duplicate(contributor=contributor)
            for collection in self
        ]


class Collection(BaseCollection):
    """Postgres model for storing a collection."""

    class Meta(BaseCollection.Meta):
        """Collection Meta options."""

        permissions = (
            ("view_collection", "Can view collection"),
            ("edit_collection", "Can edit collection"),
            ("share_collection", "Can share collection"),
            ("download_collection", "Can download files from collection"),
            ("add_collection", "Can add data objects to collection"),
            ("owner_collection", "Is owner of the collection"),
        )

    #: manager
    objects = CollectionQuerySet.as_manager()

    #: duplication date and time
    duplicated = models.DateTimeField(blank=True, null=True)

    def is_duplicate(self):
        """Return True if collection is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, contributor=None):
        """Duplicate (make a copy)."""
        duplicate = Collection.objects.get(id=self.id)
        duplicate.pk = None
        duplicate.slug = None
        duplicate.name = 'Copy of {}'.format(self.name)
        duplicate.duplicated = now()
        if contributor:
            duplicate.contributor = contributor

        duplicate.save(force_insert=True)

        assign_contributor_permissions(duplicate)

        # Fields to inherit from original data object.
        duplicate.created = self.created
        duplicate.save()

        # Duplicate collection's entities.
        entities = get_objects_for_user(contributor, 'view_entity', self.entity_set.all())  # pylint: disable=no-member
        duplicated_entities = entities.duplicate(contributor=contributor)
        duplicate.entity_set.add(*duplicated_entities)

        # Add duplicated data objects to collection.
        for duplicated_entity in duplicate.entity_set.all():
            duplicate.data.add(*duplicated_entity.data.all())

        return duplicate
