"""Resolwe entity model."""
from django.contrib.postgres.fields import ArrayField, JSONField
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

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)


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
    """Relations between entities."""

    class Meta(BaseCollection.Meta):
        """Relation Meta options."""

        permissions = (
            ("view_relation", "Can view relation"),
            ("edit_relation", "Can edit relation"),
            ("share_relation", "Can share relation"),
            ("owner_relation", "Is owner of the relation"),
        )

    #: type of the relation
    type = models.ForeignKey('RelationType', on_delete=models.PROTECT)

    #: collection in which relation is
    collection = models.ForeignKey('Collection', null=True, on_delete=models.SET_NULL)

    #: entities in the relation
    entities = models.ManyToManyField(Entity, through='PositionInRelation')

    #: optional label of the relation (i.e. to further specify relation type)
    label = models.CharField(max_length=100, null=True, blank=True)


class PositionInRelation(models.Model):
    """Intermediate model to connect Entities with Relations."""

    #: entity in the relation
    entity = models.ForeignKey(Entity, on_delete=models.CASCADE)

    #: relation to which entity belongs
    relation = models.ForeignKey(Relation, on_delete=models.CASCADE)

    #: position of the entity in the relation (relevant if `relation.type.ordered` is True)
    position = JSONField(null=True, blank=True)
