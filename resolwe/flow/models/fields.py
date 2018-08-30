"""Custom database fields."""
from django.db import connection
from django.db.models import constants
from django.db.models.fields import SlugField
from django.utils.text import slugify

from resolwe.flow.exceptions import SlugError

MAX_SLUG_SEQUENCE_DIGITS = 9


class ResolweSlugField(SlugField):
    """Slug field."""

    def __init__(self, *args, **kwargs):
        """Initialize field."""
        self.populate_from = kwargs.pop('populate_from', None)

        # unique_with value can be string or tuple
        self.unique_with = kwargs.pop('unique_with', ())
        if isinstance(self.unique_with, str):
            self.unique_with = (self.unique_with,)

        if self.unique_with:
            # we will do "manual" granular check below
            kwargs['unique'] = False

        super().__init__(*args, **kwargs)

    def deconstruct(self):
        """Deconstruct method."""
        name, path, args, kwargs = super().deconstruct()

        if self.populate_from is not None:
            kwargs['populate_from'] = self.populate_from

        if self.unique_with != ():
            kwargs['unique_with'] = self.unique_with
            kwargs.pop('unique', None)

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
                    '`unique_with` constraint does not support lookups by related models.'
                )

            field = instance._meta.get_field(field_name)  # pylint: disable=protected-access
            field_value = getattr(instance, field_name)

            # Convert value to the database representation.
            field_db_value = field.get_prep_value(field_value)

            constraint_key = 'unique_' + field_name
            constraints_expression.append("{} = %({})s".format(
                connection.ops.quote_name(field.column),
                constraint_key
            ))
            constraints_values[constraint_key] = field_db_value

        if not constraints_expression:
            return '', []

        constraints_expression = 'AND ' + ' AND '.join(constraints_expression)
        return constraints_expression, constraints_values

    def _get_populate_from_value(self, instance):
        """Get the value from ``populate_from`` attribute."""
        if hasattr(self.populate_from, '__call__'):
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

        if slug:
            slug = slugify(slug)

        if not slug:
            slug = None

            if not self.blank:
                slug = instance._meta.model_name  # pylint: disable=protected-access
            elif not self.null:
                slug = ''

        if slug:
            # Make sure that auto generated slug with added sequence
            # won't excede maximal length.
            # Validation of predefined slugs is handled by Django.
            if not predefined_slug:
                slug = slug[:(self.max_length - MAX_SLUG_SEQUENCE_DIGITS - 1)]

            constraints_placeholder, constraints_values = self._get_unique_constraints(instance)

            instance_pk_name = instance._meta.pk.name  # pylint: disable=protected-access

            # Safe values - make sure that there is no chance of SQL injection.
            query_params = {
                'constraints_placeholder': constraints_placeholder,
                'slug_column': connection.ops.quote_name(self.column),
                'slug_len': len(slug),
                'table_name': connection.ops.quote_name(self.model._meta.db_table),  # pylint: disable=protected-access
                'pk_neq_placeholder': 'AND {} != %(instance_pk)s'.format(instance_pk_name) if instance.pk else ''
            }

            # SQL injection unsafe values - will be escaped.
            # Keys prefixed with `unique_` are reserved for `constraints_values` dict.
            query_escape_params = {
                'slug': slug,
                'slug_regex': '^{}(-[0-9]*)?$'.format(slug),
            }
            query_escape_params.update(constraints_values)
            if instance.pk:
                query_escape_params['instance_pk'] = instance.pk

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
                    """.format(**query_params),
                    params=query_escape_params
                )
                result = cursor.fetchone()[0]

            if result is not None:
                if predefined_slug:
                    raise SlugError(
                        "Slug '{}' (version {}) is already taken.".format(slug, instance.version)
                    )

                if len(str(result)) > MAX_SLUG_SEQUENCE_DIGITS:
                    raise SlugError(
                        "Auto-generated slug sequence too long - please choose a different slug."
                    )

                slug = '{}-{}'.format(slug, result)

            # Make the updated slug available as instance attribute.
            setattr(instance, self.name, slug)

        return slug
