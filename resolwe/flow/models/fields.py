"""Custom database fields.

The VersionField is based on code at https://github.com/mcldev/django-versionfield .
"""

from django.conf import settings
from django.db import connection, models
from django.db.models import constants
from django.utils.text import slugify

from resolwe.flow.exceptions import SlugError

# Slug field settings.
MAX_SLUG_SEQUENCE_DIGITS = 9

# Version field settings.
DEFAULT_NUMBER_BITS = getattr(settings, "VERSION_FIELD_NUMBER_BITS", (8, 8, 16))
DEFAULT_INPUT_SIZE = getattr(settings, "VERSION_FIELD_WIDGET_INPUT_SIZE", 10)
DEFAULT_VERSION_VALUE = getattr(settings, "VERSION_FIELD_DEFAULT_VALUE", "0.0.0")


class ResolweSlugField(models.fields.SlugField):
    """Slug field."""

    def __init__(self, *args, **kwargs):
        """Initialize field."""
        self.populate_from = kwargs.pop("populate_from", None)

        # unique_with value can be string or tuple
        self.unique_with = kwargs.pop("unique_with", ())
        if isinstance(self.unique_with, str):
            self.unique_with = (self.unique_with,)

        if self.unique_with:
            # we will do "manual" granular check below
            kwargs["unique"] = False

        super().__init__(*args, **kwargs)

    def deconstruct(self):
        """Deconstruct method."""
        name, path, args, kwargs = super().deconstruct()

        if self.populate_from is not None:
            kwargs["populate_from"] = self.populate_from

        if self.unique_with != ():
            kwargs["unique_with"] = self.unique_with
            kwargs.pop("unique", None)

        return name, path, args, kwargs

    def _get_unique_constraints(self, instance):
        """Return SQL filter for filtering by fields in ``unique_with`` attribute.

        Filter is returned as tuple of two elements where first one is
        placeholder which is safe to insert into SQL query and second
        one may include potentially dangerous values and must be passed
        to SQL query in ``params`` attribute to make sure it is properly
        escaped.
        """
        constraints_expression = []
        constraints_values = {}
        for field_name in self.unique_with:
            if constants.LOOKUP_SEP in field_name:
                raise NotImplementedError(
                    "`unique_with` constraint does not support lookups by related models."
                )

            field = instance._meta.get_field(field_name)
            field_value = getattr(instance, field_name)

            # Convert value to the database representation.
            field_db_value = field.get_prep_value(field_value)

            constraint_key = "unique_" + field_name
            constraints_expression.append(
                "{} = %({})s".format(
                    connection.ops.quote_name(field.column), constraint_key
                )
            )
            constraints_values[constraint_key] = field_db_value

        if not constraints_expression:
            return "", []

        constraints_expression = "AND " + " AND ".join(constraints_expression)
        return constraints_expression, constraints_values

    def _get_populate_from_value(self, instance):
        """Get the value from ``populate_from`` attribute."""
        if hasattr(self.populate_from, "__call__"):
            # ResolweSlugField(populate_from=lambda instance: ...)
            return self.populate_from(instance)
        else:
            # ResolweSlugField(populate_from='foo')
            attr = getattr(instance, self.populate_from)
            return attr() if callable(attr) else attr

    def pre_save(self, instance, add):
        """Ensure slug uniqunes before save."""
        slug = self.value_from_object(instance)

        # We don't want to change slug defined by user.
        predefined_slug = bool(slug)

        if not slug and self.populate_from:
            slug = self._get_populate_from_value(instance)

        if original_slug := slug:
            slug = slugify(slug)
            if predefined_slug and original_slug != slug:
                raise SlugError(f"The value {original_slug} is not a valid slug.")

        if not slug:
            slug = None

            if not self.blank:
                slug = instance._meta.model_name
            elif not self.null:
                slug = ""

        if slug:
            # Make sure that auto generated slug with added sequence
            # won't excede maximal length.
            # Validation of predefined slugs is handled by Django.
            if not predefined_slug:
                slug = slug[: (self.max_length - MAX_SLUG_SEQUENCE_DIGITS - 1)]

            constraints_placeholder, constraints_values = self._get_unique_constraints(
                instance
            )

            instance_pk_name = instance._meta.pk.name

            # Safe values - make sure that there is no chance of SQL injection.
            query_params = {
                "constraints_placeholder": constraints_placeholder,
                "slug_column": connection.ops.quote_name(self.column),
                "slug_len": len(slug),
                "table_name": connection.ops.quote_name(self.model._meta.db_table),
                "pk_neq_placeholder": (
                    "AND {} != %(instance_pk)s".format(instance_pk_name)
                    if instance.pk
                    else ""
                ),
            }

            # SQL injection unsafe values - will be escaped.
            # Keys prefixed with `unique_` are reserved for `constraints_values` dict.
            query_escape_params = {
                "slug": slug,
                "slug_regex": "^{}(-[0-9]*)?$".format(slug),
            }
            query_escape_params.update(constraints_values)
            if instance.pk:
                query_escape_params["instance_pk"] = instance.pk

            with connection.cursor() as cursor:
                # TODO: Slowest part of this query is `MAX` function. It can
                #       be optimized by indexing slug column by slug sequence.
                #       https://www.postgresql.org/docs/9.4/static/indexes-expressional.html
                cursor.execute(
                    """
                    SELECT
                        CASE
                            WHEN (
                                EXISTS(
                                    SELECT 1 FROM {table_name} WHERE (
                                        {slug_column} = %(slug)s
                                        {pk_neq_placeholder}
                                        {constraints_placeholder}
                                    )
                                )
                            ) THEN MAX(slug_sequence) + 1
                            ELSE NULL
                        END
                    FROM (
                        SELECT COALESCE(
                            NULLIF(
                                RIGHT({slug_column}, -{slug_len}-1),
                                ''
                            ),
                            '1'
                        )::text::integer AS slug_sequence
                        FROM {table_name} WHERE (
                            {slug_column} ~ %(slug_regex)s
                            {pk_neq_placeholder}
                            {constraints_placeholder}
                        )
                    ) AS tmp
                    """.format(
                        **query_params
                    ),
                    params=query_escape_params,
                )
                result = cursor.fetchone()[0]

            if result is not None:
                if predefined_slug:
                    raise SlugError(
                        "Slug '{}' (version {}) is already taken.".format(
                            slug, instance.version
                        )
                    )

                if len(str(result)) > MAX_SLUG_SEQUENCE_DIGITS:
                    raise SlugError(
                        "Auto-generated slug sequence too long - please choose a different slug."
                    )

                slug = "{}-{}".format(slug, result)

            # Make the updated slug available as instance attribute.
            setattr(instance, self.name, slug)

        return slug


def convert_version_string_to_int(string: str, number_bits: list[int]) -> int:
    """Convert string version to int version.

    Take in a verison string e.g. '3.0.1'
    Store it as a converted int:
    3 * (2**number_bits[0]) + 0 * (2**number_bits[1]) + 1 * (2**number_bits[2])

    >>> convert_version_string_to_int('3.0.1',[8,8,16])
    50331649
    """
    # This is needed for using Version as a type in pydantic
    if string == Ellipsis:
        return string

    numbers = [int(number_string) for number_string in string.split(".")]

    if len(numbers) > len(number_bits):
        raise NotImplementedError(
            "Versions with more than {0} decimal places are not supported".format(
                len(number_bits) - 1
            )
        )

    # add 0s for missing numbers
    numbers.extend([0] * (len(number_bits) - len(numbers)))

    # convert to single int and return
    number = 0
    total_bits = 0
    for num, bits in reversed(list(zip(numbers, number_bits))):
        max_num = (bits + 1) - 1
        if num >= 1 << max_num:
            raise ValueError(
                "Number {0} cannot be stored with only {1} bits. Max is {2}".format(
                    num, bits, max_num
                )
            )
        number += num << total_bits
        total_bits += bits

    return number


def convert_version_int_to_string(number: int, number_bits: list[int]) -> str:
    """Convert int version to string version.

    >>> convert_version_int_to_string(50331649,[8,8,16])
    '3.0.1'
    """
    number_strings = []
    total_bits = sum(number_bits)
    for bits in number_bits:
        shift_amount = total_bits - bits
        number_segment = number >> shift_amount
        number_strings.append(str(number_segment))
        total_bits = total_bits - bits
        number = number - (number_segment << shift_amount)
    return ".".join(number_strings)


class Version:
    """The version object, used by the VersionField."""

    def __init__(self, string, number_bits=DEFAULT_NUMBER_BITS):
        """Create a version object from a string."""
        self.number_bits = number_bits
        self.internal_integer = convert_version_string_to_int(string, number_bits)

    def __str__(self):
        """Return a string representation."""
        return str(
            convert_version_int_to_string(self.internal_integer, self.number_bits)
        )

    def __repr__(self):
        """Return an internal string representation."""
        return self.__str__()

    def __int__(self):
        """Return the internal integer representation."""
        return self.internal_integer

    def __eq__(self, other):
        """Override equality operator."""
        if not other:
            return False  # we are obviously a valid Version, but 'other' isn't
        if other == Ellipsis:
            return False  # For pydantic use
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return int(self) == int(other)

    def __hash__(self):
        """Use the internal integer representation as hashing function."""
        return int(self)

    def __lt__(self, other):
        """Override less than operator."""
        if not other:
            return False
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return int(self) < int(other)

    def __le__(self, other):
        """Override less than or equal operator."""
        if not other:
            return False
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return int(self) <= int(other)

    def __gt__(self, other):
        """Override greater than operator."""
        if not other:
            return False
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return int(self) > int(other)

    def __ge__(self, other):
        """Override greater than or equal operator."""
        if not other:
            return False
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return int(self) >= int(other)

    def __add__(self, other):
        """Override add operator.

        For example: 1.0.1 + 2.0.1 = 3.0.2.
        """
        if not other:
            return self
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return Version(
            convert_version_int_to_string(int(self) + int(other), self.number_bits),
            self.number_bits,
        )

    def __sub__(self, other):
        """Override subtract operator.

        For example: 3.0.2 - 2.0.1 = 1.0.1. The minimal version is 0.0.0.
        """
        if not other:
            return self
        if isinstance(other, str):
            other = Version(other, self.number_bits)
        return Version(
            convert_version_int_to_string(
                max(int(self) - int(other), 0), self.number_bits
            ),
            self.number_bits,
        )


class VersionField(models.Field):
    """Store versions in field as strings.

    A Field where version numbers are input/output as strings (e.g. 3.0.1) but stored
    in the db as converted integers for fast indexing.
    """

    description = "A version number (e.g. 3.0.1)"

    def __init__(self, number_bits=DEFAULT_NUMBER_BITS, *args, **kwargs):
        """Initialize the field."""
        self.number_bits = number_bits
        super().__init__(*args, **kwargs)

    def db_type(self, connection):
        """Use integer as internal representation."""
        return "integer"

    def to_python(self, value):
        """Convert data to the instance of the Version object."""
        if not value:
            return None

        if isinstance(value, Version):
            return value

        if isinstance(value, str):
            return Version(value, self.number_bits)

        return Version(
            convert_version_int_to_string(value, self.number_bits), self.number_bits
        )

    def from_db_value(self, value, expression, connection):
        """Convert data from database representation."""
        if value is None:
            return value
        return Version(
            convert_version_int_to_string(value, self.number_bits), self.number_bits
        )

    def get_prep_value(self, value):
        """Convert data to the database representation."""
        if isinstance(value, str):
            return int(Version(value, self.number_bits))

        if value is None:
            return None

        return int(value)
