"""Resolwe entity model."""
from django.contrib.postgres.fields import CICharField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models, transaction

from resolwe.observers.protocol import post_permission_changed, pre_permission_changed
from resolwe.permissions.models import PermissionObject, PermissionQuerySet

from .base import BaseModel, BaseQuerySet
from .collection import BaseCollection
from .utils import DirtyError, bulk_duplicate, validate_schema


class EntityQuerySet(BaseQuerySet, PermissionQuerySet):
    """Query set for ``Entity`` objects."""

    @transaction.atomic
    def duplicate(self, contributor, inherit_collection=False):
        """Duplicate (make a copy) ``Entity`` objects.

        :param contributor: Duplication user
        :param inherit_collection: If ``True`` then duplicated
            entities will be added to collection the original entity
            is part of. Duplicated entities' data objects will also be
            added to the collection, but only those which are in the
            collection
        :return: A list of duplicated entities
        """
        return bulk_duplicate(
            entities=self,
            contributor=contributor,
            inherit_collection=inherit_collection,
        )

    @transaction.atomic
    def move_to_collection(self, destination_collection):
        """Move entities to destination collection."""
        for entity in self:
            entity.move_to_collection(destination_collection)


class Entity(BaseCollection, PermissionObject):
    """Postgres model for storing entities."""

    class Meta(BaseCollection.Meta):
        """Entity Meta options."""

        permissions = (
            ("view", "Can view entity"),
            ("edit", "Can edit entity"),
            ("share", "Can share entity"),
            ("owner", "Is owner of the entity"),
        )

        indexes = [
            models.Index(name="idx_entity_name", fields=["name"]),
            GinIndex(
                name="idx_entity_name_trgm", fields=["name"], opclasses=["gin_trgm_ops"]
            ),
            models.Index(name="idx_entity_slug", fields=["slug"]),
            GinIndex(name="idx_entity_tags", fields=["tags"]),
            GinIndex(name="idx_entity_search", fields=["search"]),
        ]

    #: manager
    objects = EntityQuerySet.as_manager()

    #: collection to which entity belongs
    collection = models.ForeignKey(
        "Collection", blank=True, null=True, on_delete=models.CASCADE
    )

    #: entity type
    type = models.CharField(max_length=100, db_index=True, null=True, blank=True)

    #: duplication date and time
    duplicated = models.DateTimeField(blank=True, null=True)

    def is_duplicate(self):
        """Return True if entity is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, contributor, inherit_collection=False):
        """Duplicate (make a copy)."""
        return bulk_duplicate(
            entities=self._meta.model.objects.filter(pk=self.pk),
            contributor=contributor,
            inherit_collection=inherit_collection,
        )[0]

    @transaction.atomic
    def move_to_collection(self, collection):
        """Move entity from the source to the destination collection.

        :args collection: the collection to move entity into.
        """
        if self.collection == collection:
            return

        if collection is None:
            raise ValidationError(
                f"Entity {self}({self.pk}) can only be moved to another container."
            )
        pre_permission_changed.send(sender=type(self), instance=self)
        self.collection = collection
        self.tags = collection.tags
        self.permission_group = collection.permission_group
        self.save(update_fields=["collection", "tags", "permission_group"])
        post_permission_changed.send(sender=type(self), instance=self)
        self.data.move_to_collection(collection)


class RelationType(models.Model):
    """Model for storing relation types."""

    #: relation type name
    name = models.CharField(max_length=100, unique=True)

    #: indicates if order of entities in relation is important or not
    ordered = models.BooleanField(default=False)

    def __str__(self):
        """Format the object representation."""
        return self.name


class Relation(BaseModel, PermissionObject):
    """Relations between entities.

    The Relation model defines the associations and dependencies between
    entities in a given collection:

    .. code-block:: json

        {
            "collection": "<collection_id>",
            "type": "comparison",
            "category": "case-control study",
            "entities": [
                {"enetity": "<entity1_id>", "label": "control"},
                {"enetity": "<entity2_id>", "label": "case"},
                {"enetity": "<entity3_id>", "label": "case"}
            ]
        }

    Relation ``type`` defines a specific set of associations among
    entities. It can be something like ``group``, ``comparison`` or
    ``series``. The relation ``type`` is an instance of
    :class:`.RelationType` and should be defined in any Django app that
    uses relations (e.g., as a fixture). Multiple relations of the same
    type are allowed on the collection.

    Relation ``category`` defines a specific use case. The relation
    category must be unique in a collection, so that users can
    distinguish between different relations. In the example above, we
    could add another ``comparison`` relation of ``category``, say
    ``Case-case study`` to compare ``<entity2>`` with ``<entity3>``.

    Relation is linked to :class:`resolwe.flow.models.Collection` to
    enable defining different relations structures in different
    collections. This also greatly speed up retrieving of relations, as
    they are envisioned to be mainly used on a collection level.

    ``unit`` defines units used in partitions where it is applicable,
    e.g. in relations of type ``series``.

    """

    UNIT_SECOND = "s"
    UNIT_MINUTE = "min"
    UNIT_HOUR = "hr"
    UNIT_DAY = "d"
    UNIT_WEEK = "wk"
    UNIT_CHOICES = (
        (UNIT_SECOND, "Second"),
        (UNIT_MINUTE, "Minute"),
        (UNIT_HOUR, "Hour"),
        (UNIT_DAY, "Day"),
        (UNIT_WEEK, "Week"),
    )

    #: type of the relation
    type = models.ForeignKey("RelationType", on_delete=models.PROTECT)

    #: collection to which relation belongs
    collection = models.ForeignKey("Collection", on_delete=models.CASCADE)

    #: partitions of entities in the relation
    entities = models.ManyToManyField(Entity, through="RelationPartition")

    #: category of the relation
    category = CICharField(max_length=100)

    #: unit used in the partitions' positions (where applicable, e.g. for serieses)
    unit = models.CharField(max_length=3, choices=UNIT_CHOICES, null=True, blank=True)

    #: relation descriptor schema
    descriptor_schema = models.ForeignKey(
        "flow.DescriptorSchema", blank=True, null=True, on_delete=models.PROTECT
    )

    #: relation descriptor
    descriptor = models.JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    #: custom manager with permission filtering methods
    objects = PermissionQuerySet.as_manager()

    class Meta(BaseModel.Meta):
        """Relation Meta options."""

        unique_together = ("collection", "category")

        permissions = (
            ("view", "Can view relation"),
            ("edit", "Can edit relation"),
            ("share", "Can share relation"),
            ("owner", "Is owner of the relation"),
        )

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


class RelationPartition(models.Model):
    """Intermediate model to define Relations between Entities."""

    #: entity in the relation
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE)

    #: relation to which entity belongs
    relation = models.ForeignKey(Relation, on_delete=models.CASCADE)

    #: number used for ordering entities in the relation
    position = models.PositiveSmallIntegerField(null=True, blank=True, db_index=True)

    #: Human-readable label of the position
    label = models.CharField(max_length=30, null=True, blank=True, db_index=True)

    class Meta:
        """RelationPartition Meta options."""

        unique_together = ("entity", "relation")
