"""Resolwe models render utils."""
from resolwe.flow.utils import dict_dot, iterate_schema


def render_descriptor(data):
    """Render data descriptor.

    The rendering is based on descriptor schema and input context.

    :param data: data instance
    :type data: :class:`resolwe.flow.models.Data` or :class:`dict`

    """
    if not data.descriptor_schema:
        return

    # Set default values
    for field_schema, field, path in iterate_schema(
        data.descriptor, data.descriptor_schema.schema, "descriptor"
    ):
        if "default" in field_schema and field_schema["name"] not in field:
            dict_dot(data, path, field_schema["default"])


def render_template(process, template_string, context):
    """Render template using the specified expression engine."""
    from resolwe.flow.managers import manager

    # Get the appropriate expression engine. If none is defined, do not evaluate
    # any expressions.
    expression_engine = process.requirements.get("expression-engine", None)
    if not expression_engine:
        return template_string

    return manager.get_expression_engine(expression_engine).evaluate_block(
        template_string, context
    )


def json_path_components(path):
    """Convert JSON path to individual path components.

    :param path: JSON path, which can be either an iterable of path
        components or a dot-separated string
    :return: A list of path components
    """
    if isinstance(path, str):
        path = path.split(".")

    return list(path)


def fill_with_defaults(process_input, input_schema):
    """Fill empty optional fields in input with default values."""
    for field_schema, fields, path in iterate_schema(
        process_input, input_schema, include_groups=True
    ):
        if "group" in field_schema and field_schema["name"] not in fields:
            dict_dot(process_input, path, {})
        if "default" in field_schema and field_schema["name"] not in fields:
            dict_dot(process_input, path, field_schema["default"])
