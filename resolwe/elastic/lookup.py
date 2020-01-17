"""Query lookup expressions."""
from elasticsearch_dsl.query import Q, Range

# Token separator for parsing lookup expressions.
TOKEN_SEPARATOR = "__"


@staticmethod
def maybe_number(value):
    """Convert number strings to numbers, pass through non-numbers."""
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


class Lookup:
    """Lookup expression."""

    operator = None
    query_type = None
    value_type = None

    def get_value_query(self, value):
        """Prepare value for use in a query."""
        if self.value_type is None:
            return value

        return self.value_type(value)

    def apply(self, search, field, value):
        """Apply lookup expression to search query."""
        return search.query(
            self.query_type(**{field: {self.operator: self.get_value_query(value)}})
        )


class LessThan(Lookup):
    """Less than lookup."""

    operator = "lt"
    query_type = Range
    value_type = maybe_number


class LessThanOrEqual(Lookup):
    """Less than or equal lookup."""

    operator = "lte"
    query_type = Range
    value_type = maybe_number


class GreaterThan(Lookup):
    """Greater than lookup."""

    operator = "gt"
    query_type = Range
    value_type = maybe_number


class GreaterThanOrEqual(Lookup):
    """Greater than or equal lookup."""

    operator = "gte"
    query_type = Range
    value_type = maybe_number


class In(Lookup):
    """In lookup."""

    operator = "in"

    def apply(self, search, field, value):
        """Apply lookup expression to search query."""
        if not isinstance(value, list):
            value = [x for x in value.strip().split(",") if x]

        filters = [Q("match", **{field: item}) for item in value]
        return search.query("bool", should=filters)


class Exact(Lookup):
    """Exact lookup."""

    operator = "exact"

    def apply(self, search, field, value):
        """Apply lookup expression to search query."""
        # We assume that the field in question has a "raw" counterpart.
        return search.query("match", **{"{}.raw".format(field): value})


class QueryBuilder:
    """Query builder."""

    def __init__(self, fields, fields_map, custom_filter_object):
        """Construct query builder.

        :param fields: List of supported fields
        :param fields_map: Field alias mapping
        :param custom_filter_object: Object for calling custom overrides
        """
        self.fields = fields
        self.fields_map = fields_map
        self.custom_filter_object = custom_filter_object
        self._lookups = {}

        # Register default lookups.
        self.register_lookup(LessThan)
        self.register_lookup(LessThanOrEqual)
        self.register_lookup(GreaterThan)
        self.register_lookup(GreaterThanOrEqual)
        self.register_lookup(In)
        self.register_lookup(Exact)

    def register_lookup(self, lookup):
        """Register lookup."""
        if lookup.operator in self._lookups:
            raise KeyError(
                "Lookup for operator '{}' is already registered".format(lookup.operator)
            )

        self._lookups[lookup.operator] = lookup()

    def get_lookup(self, operator):
        """Look up a lookup.

        :param operator: Name of the lookup operator
        """
        try:
            return self._lookups[operator]
        except KeyError:
            raise NotImplementedError(
                "Lookup operator '{}' is not supported".format(operator)
            )

    def build(self, search, raw_query):
        """Build query.

        :param search: Search query instance
        :param raw_query: Raw query arguments dictionary
        """
        unmatched_items = {}
        for expression, value in raw_query.items():
            # Parse query expression into tokens.
            tokens = expression.split(TOKEN_SEPARATOR)

            field = tokens[0]
            tail = tokens[1:]

            if field not in self.fields:
                unmatched_items[expression] = value
                continue

            # Map field alias to final field.
            field = self.fields_map.get(field, field)

            # Parse lookup expression. Currently only no token or a single token is allowed.
            if tail:
                if len(tail) > 1:
                    raise NotImplementedError(
                        "Nested lookup expressions are not supported"
                    )

                lookup = self.get_lookup(tail[0])
                search = lookup.apply(search, field, value)
            else:
                # Default lookup.
                custom_filter = getattr(
                    self.custom_filter_object, "custom_filter_{}".format(field), None
                )
                if custom_filter is not None:
                    search = custom_filter(value, search)
                elif isinstance(value, list):
                    # Default is 'should' between matches. If you need anything else,
                    # a custom filter for this field should be implemented.
                    filters = [Q("match", **{field: item}) for item in value]
                    search = search.query("bool", should=filters)
                else:
                    search = search.query(
                        "match", **{field: {"query": value, "operator": "and"}}
                    )

        return (search, unmatched_items)
