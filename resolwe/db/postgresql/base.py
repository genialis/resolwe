"""Custom database wrapper for PostgreSQL.

Django uses ``LIKE UPPER(%)`` operator for ``icontains``,
``istartswith`` and ``iendswith`` filters. This operator doesn't work
with ``gin_trgm_ops`` index like ``LIKE %s`` operator does - or at least
it requires a separate index built with ``UPPER``.

Better (and faster) solution is to use ``ILIKE`` operator which uses
the same index as ``LIKE``.

Some background for Django's design is available at:
https://code.djangoproject.com/ticket/3575
"""

from django.db.backends.postgresql import base, operations


class DatabaseOperations(
    operations.DatabaseOperations
):  # pylint: disable=abstract-method
    """Database operations class."""

    def lookup_cast(self, lookup_type, internal_type=None):
        """Copy of the original method, but don't use ``UPPER`` for i-lookups."""
        lookup = "%s"

        # Cast text lookups to text to allow things like filter(x__contains=4)
        if lookup_type in (
            "iexact",
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "endswith",
            "iendswith",
            "regex",
            "iregex",
        ):
            if internal_type in ("IPAddressField", "GenericIPAddressField"):
                lookup = "HOST(%s)"
            else:
                lookup = "%s::text"

        return lookup


class DatabaseWrapper(base.DatabaseWrapper):
    """Database wrapper for PostgreSQL database.

    Inherit from Django's defaulf database wrapper and apply some
    changes to how the SQL queries are composited.
    """

    ops_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        """Initialize the wrapper."""
        self.operators.update(
            {
                "iexact": "ILIKE %s",
                "icontains": "ILIKE %s",
                "istartswith": "ILIKE %s",
                "iendswith": "ILIKE %s",
            }
        )
        self.pattern_ops.update(
            {
                "iexact": "ILIKE {}",
                "icontains": "ILIKE '%%' || {} || '%%'",
                "istartswith": "ILIKE {} || '%%'",
                "iendswith": "ILIKE '%%' || {}",
            }
        )
        super().__init__(*args, **kwargs)
