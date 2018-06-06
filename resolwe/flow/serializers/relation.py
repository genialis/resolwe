"""Resolwe relation serializer."""
from django.db import transaction

from rest_framework import serializers

from resolwe.flow.models.collection import Collection
from resolwe.flow.models.entity import PositionInRelation, Relation, RelationType
from resolwe.rest.fields import ProjectableJSONField
from resolwe.rest.serializers import SelectiveFieldMixin

from .base import ResolweBaseSerializer


class PositionInRelationSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Serializer for PositionInRelation objects."""

    position = ProjectableJSONField(allow_null=True, required=False)

    class Meta:
        """PositionInRelationSerializer Meta options."""

        model = PositionInRelation
        fields = ('entity', 'position')


class RelationSerializer(ResolweBaseSerializer):
    """Serializer for Relation objects."""

    entities = PositionInRelationSerializer(source='positioninrelation_set', many=True)
    collection = serializers.PrimaryKeyRelatedField(queryset=Collection.objects.all(), required=True)

    class Meta:
        """RelationSerializer Meta options."""

        model = Relation
        read_only_fields = (
            'created',
            'id',
            'modified',
        )
        update_protected_fields = (
            'contributor',
            'entities',
            'type',
        )
        fields = read_only_fields + update_protected_fields + (
            'collection',
            'label',
        )

    def get_fields(self):
        """Dynamically adapt fields based on the current request."""
        fields = super(RelationSerializer, self).get_fields()

        if self.request.method == "GET":
            fields['type'] = serializers.CharField(source='type.name')
        else:
            fields['type'] = serializers.PrimaryKeyRelatedField(queryset=RelationType.objects.all())

        return fields

    def create(self, validated_data):
        """Create ``Relation`` object and add ``Entities``."""
        # `entities` field is renamed to `positioninrelation_set` based on source of nested serializer

        entities = validated_data.pop('positioninrelation_set', {})

        # Prevent "Failed to populate slug Relation.slug from name" output
        validated_data['name'] = 'Relation'

        with transaction.atomic():
            instance = Relation.objects.create(**validated_data)
            for entity in entities:
                PositionInRelation.objects.create(
                    relation=instance,
                    entity=entity['entity'],
                    position=entity.get('position', None)
                )

        return instance
