"""Implementation of field projection."""
from collections.abc import Mapping, Sequence

FIELD_SEPARATOR = ","
FIELD_DEREFERENCE = "__"


def apply_subfield_projection(field, value, deep=False):
    """Apply projection from request context.

    The passed dictionary may be mutated.

    :param field: An instance of `Field` or `Serializer`
    :type field: `Field` or `Serializer`
    :param value: Dictionary to apply the projection to
    :type value: dict
    :param deep: Also process all deep projections
    :type deep: bool
    """
    # Discover the root manually. We cannot use either `self.root` or `self.context`
    # due to a bug with incorrect caching (see DRF issue #5087).
    prefix = []
    root = field
    while root.parent is not None:
        # Skip anonymous serializers (e.g., intermediate ListSerializers).
        if root.field_name:
            prefix.append(root.field_name)
        root = root.parent
    prefix = prefix[::-1]

    context = getattr(root, "_context", {})

    # If there is no request, we cannot perform filtering.
    request = context.get("request")
    if request is None:
        return value

    filtered = set(request.query_params.get("fields", "").split(FIELD_SEPARATOR))
    filtered.discard("")
    if not filtered:
        # If there are no fields specified in the filter, return all fields.
        return value

    # Extract projection for current and deeper levels.
    current_level = len(prefix)
    current_projection = []
    for item in filtered:
        item = item.split(FIELD_DEREFERENCE)
        if len(item) <= current_level:
            continue

        if item[:current_level] == prefix:
            if deep:
                current_projection.append(item[current_level:])
            else:
                current_projection.append([item[current_level]])

    if deep and not current_projection:
        # For deep projections, an empty projection means that all fields should
        # be returned without any projection.
        return value

    # Apply projection.
    return apply_projection(current_projection, value)


def apply_projection(projection, value):
    """Apply projection."""
    if isinstance(value, Sequence):
        # Apply projection to each item in the list.
        return [apply_projection(projection, item) for item in value]
    elif not isinstance(value, Mapping):
        # Non-dictionary values are simply ignored.
        return value

    # Extract projection for current level.
    try:
        current_projection = [p[0] for p in projection]
    except IndexError:
        return value

    # Apply projection.
    for name in list(value.keys()):
        if name not in current_projection:
            value.pop(name)
        elif isinstance(value[name], dict):
            # Apply projection recursively.
            value[name] = apply_projection(
                [p[1:] for p in projection if p[0] == name], value[name]
            )

    return value
