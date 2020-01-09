"""Resolwe entity model."""
from django.contrib.postgres.fields import CICharField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models, transaction
from django.utils.timezone import now

from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import assign_contributor_permissions, copy_permissions

from .base import BaseModel, BaseQuerySet
from .collection import BaseCollection


class EntityQuerySet(BaseQuerySet):
    """Query set for ``Entity`` objects."""

    @transaction.atomic
    def duplicate(self, contributor=None, inherit_collection=False):
        """Duplicate (make a copy) ``Entity`` objects.

        :param contributor: Duplication user
        :param inherit_collection: If ``True`` then duplicated
            entities will be added to collection the original entity
            is part of. Duplicated entities' data objects will also be
            added to the collection, but only those which are in the
            collection
        :return: A list of duplicated entities
        """
        return [entity.duplicate(contributor, inherit_collection) for entity in self]

    @transaction.atomic
    def move_to_collection(self, source_collection, destination_collection):
        """Move entities from source to destination collection."""
        for entity in self:
            entity.move_to_collection(source_collection, destination_collection)


class Entity(BaseCollection):
    """Postgres model for storing entities."""

    class Meta(BaseCollection.Meta):
        """Entity Meta options."""

        permissions = (
            ("view_entity", "Can view entity"),
            ("edit_entity", "Can edit entity"),
            ("share_entity", "Can share entity"),
            ("owner_entity", "Is owner of the entity"),
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

    def duplicate(self, contributor=None, inherit_collection=False):
        """Duplicate (make a copy)."""
        duplicate = Entity.objects.get(id=self.id)
        duplicate.pk = None
        duplicate.slug = None
        duplicate.name = "Copy of {}".format(self.name)
        duplicate.duplicated = now()
        if contributor:
            duplicate.contributor = contributor

        duplicate.collection = None
        if inherit_collection:
            if not contributor.has_perm("edit_collection", self.collection):
                raise ValidationError(
                    "You do not have edit permission on collection {}.".format(
                        self.collection
                    )
                )
            duplicate.collection = self.collection

        duplicate.save(force_insert=True)

        assign_contributor_permissions(duplicate)

        # Override fields that are automatically set on create.
        duplicate.created = self.created
        duplicate.save()

        # Duplicate entity's data objects.
        data = get_objects_for_user(contributor, "view_data", self.data.all())
        duplicated_data = data.duplicate(
            contributor, inherit_collection=inherit_collection
        )
        duplicate.data.add(*duplicated_data)

        # Permissions
        assign_contributor_permissions(duplicate)
        copy_permissions(duplicate.collection, duplicate)

        return duplicate

    @transaction.atomic
    def move_to_collection(self, source_collection, destination_collection):
        """Move entity to destination collection."""
        if source_collection == destination_collection:
            return

        self.collection = destination_collection
        if destination_collection:
            self.tags = destination_collection.tags
            copy_permissions(destination_collection, self)
        self.save()

        for datum in self.data.all():
            datum.collection = destination_collection
            if destination_collection:
                datum.tags = destination_collection.tags
                copy_permissions(destination_collection, datum)
            datum.save()


class RelationType(models.Model):
    """Model for storing relation types."""

    #: relation type name
    name = models.CharField(max_length=100, unique=True)

    #: indicates if order of entities in relation is important or not
    ordered = models.BooleanField(default=False)

    def __str__(self):
        """Format the object representation."""
        return self.name


class Relation(BaseModel):
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

    class Meta(BaseModel.Meta):
        """Relation Meta options."""

        unique_together = ("collection", "category")

        permissions = (
            ("view_relation", "Can view relation"),
            ("edit_relation", "Can edit relation"),
            ("share_relation", "Can share relation"),
            ("owner_relation", "Is owner of the relation"),
        )


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
