"""Resolwe annotations model."""
import abc
import datetime
import re
from collections import defaultdict
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
)

from django.core.exceptions import ValidationError
from django.db import models

from resolwe.permissions.models import PermissionObject

if TYPE_CHECKING:
    from resolwe.flow.models import Entity, Collection

from .base import AuditModel


class HandleMissingAnnotations(Enum):
    """How to handle missing annotations."""

    ADD = "ADD"
    REMOVE = "REMOVE"


class AnnotationType(Enum):
    """Supported annotation types."""

    DATE = "DATE"
    DECIMAL = "DECIMAL"
    INTEGER = "INTEGER"
    STRING = "STRING"


class AnnotationValueValidator:
    """Class that validates the annotation value.

    It uses plugins (classes) to validate the annotation of the given type.
    """

    def __init__(self):
        """Initialize."""
        self._validators: Mapping[
            AnnotationType, MutableSequence["AnnotationFieldBaseValidator"]
        ] = defaultdict(list)

    def _add_validator(self, validator: "AnnotationFieldBaseValidator"):
        """Add the given validator."""
        for field_type in validator.annotation_field_types:
            self._validators[field_type].append(validator)

    def validate(
        self, annotation_value: "AnnotationValue", raise_exception: bool = False
    ) -> Sequence[str]:
        """Validate the given AnnotationValue object.

        All validation errors are grouped together.

        :return: sequence of validation errors.
        :raises ValidationError: if annotation fails and raise_exception is True. The
            actual errors are nested inside outer ValidationError.
        """
        errors = []
        validators = self._validators[annotation_value.field.annotation_type]
        for validator in validators:
            try:
                validator.validate(annotation_value.value, annotation_value.field)
            except ValidationError as error:
                errors.append(error)
        if errors and raise_exception:
            raise ValidationError(errors)
        return errors


annotation_value_validator = AnnotationValueValidator()


class AnnotationFieldBaseValidator(metaclass=abc.ABCMeta):
    """Base class for annotation class validation."""

    annotation_field_types: Iterable[AnnotationType]

    @classmethod
    def __init_subclass__(cls: Type["AnnotationFieldBaseValidator"], **kwargs):
        """Register instance of the class with the validator."""
        super().__init_subclass__(**kwargs)
        annotation_value_validator._add_validator(cls())

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate the value.

        :raises ValidatiorError: on validation failure.
        """
        raise NotImplementedError("Subclass should implement the 'validate' method.")


class AnnotationFieldStringValidator(AnnotationFieldBaseValidator):
    """Validates the string field."""

    annotation_field_types = (AnnotationType.STRING,)

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate value to be of type str."""
        if not isinstance(value, str):
            raise ValidationError(
                f"The value '{value}' is not of the expected type 'str'."
            )


class AnnotationFieldIntegerValidator(AnnotationFieldBaseValidator):
    """Validates the integer field."""

    annotation_field_types = (AnnotationType.INTEGER,)

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate value to be of type int."""
        if not isinstance(value, int):
            raise ValidationError(
                f"The value '{value}' is not of the expected type 'int'."
            )


class AnnotationFieldDecimalValidator(AnnotationFieldBaseValidator):
    """Validates the decimal field."""

    annotation_field_types = (AnnotationType.DECIMAL,)

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate value to be of type int or float."""
        if not isinstance(value, (int, float)):
            raise ValidationError(
                f"The value '{value}' is not of the expected types 'int' or 'float'."
            )


class AnnotationFieldDateValidator(AnnotationFieldBaseValidator):
    """Validates the date field."""

    annotation_field_types = (AnnotationType.DATE,)

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate value represents a date in the format YYYY-MM-DD.

        The month and day part can contain a single character.
        """
        if not isinstance(value, str):
            raise ValidationError(
                f"The value '{value}' is not of the expected type 'str'."
            )
        try:
            datetime.datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValidationError(
                f"Value {value} has incorrect format, use YYYY-MM-DD."
            )


class AnnotationFieldRegexValidator(AnnotationFieldBaseValidator):
    """Validates regex for the annotation value."""

    annotation_field_types = (
        AnnotationType.INTEGER,
        AnnotationType.DATE,
        AnnotationType.DECIMAL,
        AnnotationType.STRING,
    )

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate that the value matches regex defined on the field.

        The value is converted to string before matching.
        """
        # Validate the regex.
        if annotation_field.validator_regex is not None:
            regex = annotation_field.validator_regex
            value = str(value)
            if re.search(regex, value) is None:
                raise ValidationError(
                    f"The value '{value}' for the field '{annotation_field.pk}' does "
                    f"not match the regex '{regex}'."
                )


class AnnotationFieldVocabularyValidator(AnnotationFieldBaseValidator):
    """Validates that the value matches vocabulary."""

    annotation_field_types = (
        AnnotationType.INTEGER,
        AnnotationType.DATE,
        AnnotationType.DECIMAL,
        AnnotationType.STRING,
    )

    def validate(self, value: Any, annotation_field: "AnnotationField"):
        """Validate that the value matches vocabulary on the field."""
        if annotation_field.vocabulary and value not in annotation_field.vocabulary:
            raise ValidationError(
                f"The value '{value}' is not valid for the field {annotation_field}."
            )


class AnnotationGroup(models.Model):
    """Group of annotation fields."""

    #: the name of the annotation group
    name = models.CharField(max_length=128)

    #: the label of the annotation group
    label = models.CharField(max_length=128)

    #: the sorting order among annotation groups
    sort_order = models.PositiveSmallIntegerField()

    def __str__(self) -> str:
        """Return user-friendly string representation."""
        return f"{self.name}"

    class Meta:
        """Set the default ordering."""

        ordering = ["sort_order"]


class AnnotationField(models.Model):
    """Annotation field."""

    #: the name of the annotation fields
    name = models.CharField(max_length=128)

    #: user visible field name
    label = models.CharField(max_length=128)

    #: user visible field description
    description = models.CharField(max_length=256)

    #: the type of the annotation field
    type = models.CharField(max_length=16)

    #: the annotation group this field belongs to
    group = models.ForeignKey(
        AnnotationGroup, on_delete=models.CASCADE, related_name="fields"
    )

    #: the sorting order among annotation fields
    sort_order = models.PositiveSmallIntegerField()

    #: optional regular expression for validation
    validator_regex = models.CharField(max_length=128, null=True)

    #: optional map of valid values to labels
    vocabulary = models.JSONField(null=True)

    def label_by_value(self, label: str) -> str:
        """Get the value by label.

        When no value is found the label is returned.
        """
        return {label: value for value, label in self.vocabulary.items()}.get(
            label, label
        )

    @staticmethod
    def add_to_collection(source: "Collection", destination: "Collection"):
        """Add fields from the source to the destination collection."""
        destination.annotation_fields.add(*source.annotation_fields.all())

    @property
    def annotation_type(self) -> AnnotationType:
        """Get the field type as enum."""
        return AnnotationType(self.type.upper())

    @staticmethod
    def group_field_from_path(path: str) -> List[str]:
        """Return the group and field name from path."""
        return path.split(".", maxsplit=1)

    @classmethod
    def id_from_path(cls, path: str) -> Optional[int]:
        """Get the field id from the field path."""
        group_name, field_name = cls.group_field_from_path(path)
        return (
            cls.objects.filter(group__name=group_name, name=field_name)
            .values_list("id", flat=True)
            .first()
        )

    @classmethod
    def field_from_path(cls, path: str) -> Optional["AnnotationField"]:
        """Get the field id from the field path."""
        group_name, field_name = cls.group_field_from_path(path)
        return cls.objects.filter(group__name=group_name, name=field_name).first()

    def __str__(self) -> str:
        """Return user-friendly string representation."""
        return f"{self.group.name}.{self.name}"

    class Meta:
        """Set the constraints and the default ordering."""

        constraints = [
            # Accept only supported annotation types.
            models.constraints.CheckConstraint(
                check=models.Q(type__in=[e.value for e in AnnotationType]),
                name="annotation_type",
            ),
            models.constraints.UniqueConstraint(
                fields=["name", "group"], name="uniquetogether_name_group"
            ),
        ]
        ordering = ["group__sort_order", "sort_order"]


class AnnotationPreset(AuditModel, PermissionObject):
    """The named set of annotation fields.

    The presets have permissions.
    """

    #: the name of the annotation fields
    name = models.CharField(max_length=128)

    #: the fields belongint to this preset
    fields = models.ManyToManyField(AnnotationField, related_name="presets")

    class Meta:
        """Override parent meta."""


class AnnotationValue(AuditModel):
    """The value of the annotation."""

    #: the entity this field belongs to
    entity: "Entity" = models.ForeignKey(
        "Entity", related_name="annotations", on_delete=models.CASCADE
    )

    #: the field this field belongs to
    field: AnnotationField = models.ForeignKey(
        AnnotationField, related_name="values", on_delete=models.CASCADE
    )

    #: value is stored under key 'value' in the json field to simplify lookups
    _value: Any = models.JSONField(default=dict)

    def __init__(self, *args, **kwargs):
        """Allow us to set the 'value' in the constructor.

        This is a little tricky, as value can be passed in args or kwargs. We only
        intercept the value when given as kwarg, since args are used by Django when
        constructing class from database value.
        """
        if "value" in kwargs:
            kwargs["_value"] = {"value": kwargs.pop("value")}
        super().__init__(*args, **kwargs)

    @property
    def value(self):
        """Get the actual value."""
        return self._value["value"]

    @value.setter
    def value(self, value):
        """Set the value.

        The object must be saved for the value to propagate to the database.
        """
        self._value["value"] = value

    def validate(self):
        """Validate the given value.

        The validation is always done in full, all errors are gathered and sent
        in the response.

        :raises ValidationError: when the validation fails.
        """
        annotation_value_validator.validate(self, raise_exception=True)

    @property
    def value_label_map(self) -> Dict[str, Any]:
        """Compute the mapping from value to label.

        Only applicable when vocabulary is given, otherwise empty dictionary is
        returned.
        """
        return self.field.vocabulary or {}

    @property
    def label(self) -> Any:
        """Get the value of the label from value.

        This is only applicable if the vocabulary is set. When no vocabulary is
        set it returns the value.
        """
        return self.field.vocabulary.get(self.value, self.value)

    def __str__(self) -> str:
        """Return user-friendly string representation."""
        return f"{self.value}"
