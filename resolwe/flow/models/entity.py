"""Resolwe entity model."""
from django.contrib.postgres.fields import CICharField
from django.db import models

from .base import BaseModel
from .collection import BaseCollection


class Entity(BaseCollection):
    """Postgres model for storing entities."""

    class Meta(BaseCollection.Meta):
        """Entity Meta options."""

        permissions = (
            ("view_entity", "Can view entity"),
            ("edit_entity", "Can edit entity"),
            ("share_entity", "Can share entity"),
            ("download_entity", "Can download files from entity"),
            ("add_entity", "Can add data objects to entity"),
            ("owner_entity", "Is owner of the entity"),
        )

    #: list of collections to which entity belongs
    collections = models.ManyToManyField('Collection')

    #: indicate whether `descriptor` is completed (set by user)
    descriptor_completed = models.BooleanField(default=False)


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

    UNIT_SECOND = 's'
    UNIT_MINUTE = 'min'
    UNIT_HOUR = 'hr'
    UNIT_DAY = 'd'
    UNIT_WEEK = 'wk'
    UNIT_CHOICES = (
        (UNIT_SECOND, "Second"),
        (UNIT_MINUTE, "Minute"),
        (UNIT_HOUR, "Hour"),
        (UNIT_DAY, "Day"),
        (UNIT_WEEK, "Week"),
    )

    #: type of the relation
    type = models.ForeignKey('RelationType', on_delete=models.PROTECT)

    #: collection to which relation belongs
    collection = models.ForeignKey('Collection', on_delete=models.CASCADE)

    #: partitions of entities in the relation
    entities = models.ManyToManyField(Entity, through='RelationPartition')

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
