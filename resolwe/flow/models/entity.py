"""Resolwe entity model."""
from typing import Any, List, Optional, Union

from django.contrib.postgres.fields import CICharField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models, transaction

from resolwe.flow.models.annotations import (
    AnnotationField,
    AnnotationValue,
    HandleMissingAnnotations,
)
from resolwe.observers.protocol import post_permission_changed, pre_permission_changed
from resolwe.permissions.models import PermissionObject, PermissionQuerySet

from .base import BaseModel, BaseQuerySet
from .collection import BaseCollection, Collection
from .utils import DirtyError, bulk_duplicate, validate_schema


class EntityQuerySet(BaseQuerySet, PermissionQuerySet):
    """Query set for ``Entity`` objects."""

    @transaction.atomic
    def duplicate(
        self, contributor, inherit_collection: bool = False
    ) -> models.QuerySet:
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
    def move_to_collection(
        self,
        destination_collection: Collection,
        missing_annotations: HandleMissingAnnotations = HandleMissingAnnotations.ADD,
    ):
        """Move entities to destination collection."""
        for entity in self:
            entity.move_to_collection(destination_collection, missing_annotations)

    def annotate_all(self, add_labels: bool = False):
        """Annotate with all metadata on the entities.

        The returned dataset has one entry for every annotation on the entity.
        It is annotated by "annotation_value" and the "annotation_field" in the
        form of f"{group_name}.{field_name}".

        :attr add_labels: when True and vocabulary is present, the annotation
            "annotation_label" contains the value of the label.
        """
        annotation_data = {
            "annotation_value": models.F("annotations__json__value"),
            "annotation_field": models.functions.Concat(
                models.F("annotations__field__group__name"),
                models.Value("."),
                models.F("annotations__field__name"),
            ),
        }
        if add_labels:
            annotation_data["annotation_label"] = models.functions.Coalesce(
                models.Func(
                    models.F("annotations__field__vocabulary"),
                    models.Func(
                        models.F("annotations__json"),
                        models.Value("value"),
                        function="jsonb_extract_path_text",
                    ),
                    function="jsonb_extract_path",
                    output_field=models.JSONField(),
                ),
                models.functions.Cast("annotations___value__value", models.JSONField()),
            )
        return self.annotate(**annotation_data)

    @classmethod
    def _prepare_annotation_data(
        cls,
        path: str,
        annotation_name: Optional[str] = None,
        value_to_label: bool = False,
        outerref_entity_pk_path: str = "pk",
    ):
        group_name, field_name = path.split(".", maxsplit=1)

        subquery = AnnotationValue.objects.filter(
            field__name=field_name,
            field__group__name=group_name,
            entity_id=models.OuterRef(outerref_entity_pk_path),
        )
        if value_to_label:
            subquery = subquery.annotate(
                final_value=models.functions.Coalesce(
                    models.Func(
                        models.F("field__vocabulary"),
                        models.Func(
                            models.F("_value"),
                            models.Value("value"),
                            function="jsonb_extract_path_text",
                        ),
                        function="jsonb_extract_path_text",
                        output_field=models.JSONField(),
                    ),
                    models.Func(
                        models.F("_value"),
                        models.Value("value"),
                        function="jsonb_extract_path_text",
                        output_field=models.JSONField(),
                    ),
                )
            )
        else:
            subquery = subquery.annotate(final_value=models.F("_value__value"))

        annotation_name = annotation_name or f"{group_name}_{field_name}"
        return {annotation_name: models.Subquery(subquery.values("final_value")[:1])}

    def annotate_path(
        self,
        paths: Union[str, List[str]],
        annotation_name: Optional[str] = None,
        value_to_label=False,
    ):
        """Add annotation to the Entity QuerySet.

        The annotation is the value of the field with the given name (in the
        given group).

        :attr paths: the path in the form 'group_name.field_name' or list of
            such paths.
        :attr field_name: the name of the annotation field.
        :attr annotation_name: the name under which annotation will be stored.
            When empty the name f"{group_name}_{field_name}" will be used. The
            annotation_name can only be used when path is a single string.
        :attr value_to_label: optionally annotate with label instead of the
            value. Only applicable when the vocabulary on the field is given.
        """
        if isinstance(paths, str):
            paths = [paths]
        queryset = self
        for path in paths:
            queryset = queryset.annotate(
                **self._prepare_annotation_data(path, annotation_name, value_to_label)
            )
        return queryset


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

    def get_annotation(self, path: str, default: Any = None) -> Any:
        """Get the annotation for the given path.

        :attr path: the path to the annotation in the format 'group.field'.
        :attr default: default value when annotation is not found.

        :return: value of the annotation or default if not found.
        """
        group_name, field_name = path.split(".", maxsplit=1)
        annotation: AnnotationValue = self.annotations.filter(
            field__group__name=group_name, field__name=field_name
        ).first()
        return annotation.value if annotation else default

    def set_annotation(self, path: str, value: Any):
        """Get the annotation for the given path.

        :attr path: the path to the annotation in the format 'group.field'.
        :attr value: the annotation value.
        """
        field_id = AnnotationField.id_from_path(path)
        if field_id is not None:
            AnnotationValue.objects.create(field_id=field_id, entity=self, value=value)
        else:
            raise RuntimeError(f"No annotation field for path {path}.")

    def is_duplicate(self):
        """Return True if entity is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, contributor, inherit_collection: bool = False) -> "Entity":
        """Duplicate (make a copy)."""
        return bulk_duplicate(
            entities=self._meta.model.objects.filter(pk=self.pk),
            contributor=contributor,
            inherit_collection=inherit_collection,
        )[0]

    @transaction.atomic
    def move_to_collection(
        self,
        collection: Collection,
        handle_missing_annotations: HandleMissingAnnotations = HandleMissingAnnotations.ADD,
    ):
        """Move entity from the source to the destination collection.

        :args collection: the collection to move entity into.
        :args handle_missing_annotations: how to handle missing annotations fields in
            the new collection

        :raises ValidationError: when collection is set to None.
        """
        if self.collection == collection:
            return

        if collection is None:
            raise ValidationError(
                f"Entity {self}({self.pk}) can only be moved to another container."
            )
        pre_permission_changed.send(sender=type(self), instance=self)

        missing_annotations = AnnotationField.objects.none()
        if self.collection is not None:
            missing_annotations = self.collection.annotation_fields.exclude(
                pk__in=collection.annotation_fields.values_list("pk", flat=True)
            )

        if missing_annotations:
            if handle_missing_annotations == HandleMissingAnnotations.ADD:
                collection.annotation_fields.add(*missing_annotations)
            elif handle_missing_annotations == HandleMissingAnnotations.REMOVE:
                self.annotations.filter(field__in=missing_annotations).delete()

        self.collection = collection
        self.tags = collection.tags
        self.permission_group = collection.permission_group
        self.save(update_fields=["collection", "tags", "permission_group"])
        post_permission_changed.send(sender=type(self), instance=self)
        self.data.move_to_collection(collection)

    def invalid_annotation_fields(self, annotation_fields=None):
        """Get the Queryset of invalid annotation fields.

        The invalid annotation field is a field that has annotatiton but it is
        not allowed in the collection this entity belongs to.

        :attr annotation_fields: the iterable containing annotations fields to
            be checked. When None is given the annotation fields belonging to
            the entity are checked.
        """
        annotation_fields = annotation_fields or self.collection.annotation_fields
        return AnnotationField.objects.filter(values=self.annotations).exclude(
            collection=self.collection
        )

    def copy_annotations(self, destination: "Entity") -> List[AnnotationValue]:
        """Copy annotation from this entity to the destination.

        :raises ValidationError: when some of the annotation fields are missing on the
            destination entity.
        """
        # Entity without collection can not have annotations.
        if self.collection is None:
            return []
        if destination.collection is None:
            raise ValidationError(
                f"The entity '{destination.slug}' must belong to the collection."
            )
        destination_annotations = []
        annotation_field_ids = set()
        # Create AnnotationValue objects from annotations and collect the set of
        # annatiton field ids for them. This set must also be available on the
        # destination entity, else exception is raised.
        for source_annotation in self.annotations.all():
            annotation_value = AnnotationValue(
                entity=destination, field_id=source_annotation.field_id
            )
            annotation_value.value = source_annotation.value
            destination_annotations.append(annotation_value)
            annotation_field_ids.add(source_annotation.field_id)

        destination_fields_qs = destination.collection.annotation_fields.filter(
            pk__in=annotation_field_ids
        )
        if destination_fields_qs.count() < len(annotation_field_ids):
            missing_annotation_field_ids = annotation_field_ids - set(
                destination_fields_qs.values_list("pk", flat=True)
            )
            missing_fields_qs = AnnotationField.objects.filter(
                pk__in=missing_annotation_field_ids
            ).values_list("group__name", "name")
            missing_field_paths = [".".join(entry) for entry in missing_fields_qs]
            missing_fields = ", ".join(missing_field_paths)
            raise ValidationError(
                f"The collection of entity '{destination}' is missing annotation fields {missing_fields}."
            )
        return destination_annotations

    def validate_annotations(self):
        """Perform streamlined descriptor validation.

        :raises ValidationError: when annotations do not pass validation. All
            fields are validated and error messages aggregated into single
            exception.
        """
        invalid_annotation_fields = self.invalid_annotation_fields()
        if invalid_annotation_fields:
            msg = ",".join(field.label for field in invalid_annotation_fields)
            collection_id = getattr(self, "collection_id", None)
            raise ValidationError(
                f"The entity '{self.pk}' is annotated with fields '{msg}' "
                f"that are not allowed on the collection '{collection_id}'."
            )

        # Validate all the annotation values on this entity.
        validation_errors = []
        for annotation in self.annotations:
            try:
                annotation.validate()
            except ValidationError as validation_error:
                validation_errors.append(validation_error)
        if validation_errors:
            raise ValidationError(validation_errors)


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
