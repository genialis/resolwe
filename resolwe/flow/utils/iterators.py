"""Iterator utils."""
import collections


def iterate_fields(fields, schema, path_prefix=None):
    """Iterate over all field values sub-fields.

    This will iterate over all field values. Some fields defined in the schema
    might not be visited.

    :param fields: field values to iterate over
    :type fields: dict
    :param schema: schema to iterate over
    :type schema: dict
    :return: (field schema, field value)
    :rtype: tuple

    """
    if path_prefix is not None and path_prefix != '' and path_prefix[-1] != '.':
        path_prefix += '.'

    schema_dict = {val['name']: val for val in schema}
    for field_id, properties in fields.items():
        path = '{}{}'.format(path_prefix, field_id) if path_prefix is not None else None
        if field_id not in schema_dict:
            raise KeyError("Field definition ({}) missing in schema".format(field_id))
        if 'group' in schema_dict[field_id]:
            for rvals in iterate_fields(properties, schema_dict[field_id]['group'], path):
                yield rvals if path_prefix is not None else rvals[:2]
        else:
            rvals = (schema_dict[field_id], fields, path)
            yield rvals if path_prefix is not None else rvals[:2]


def iterate_schema(fields, schema, path_prefix=''):
    """Iterate over all schema sub-fields.

    This will iterate over all field definitions in the schema. Some field v
    alues might be None.

    :param fields: field values to iterate over
    :type fields: dict
    :param schema: schema to iterate over
    :type schema: dict
    :param path_prefix: dot separated path prefix
    :type path_prefix: str
    :return: (field schema, field value, field path)
    :rtype: tuple

    """
    if path_prefix and path_prefix[-1] != '.':
        path_prefix += '.'

    for field_schema in schema:
        name = field_schema['name']
        if 'group' in field_schema:
            for rvals in iterate_schema(fields[name] if name in fields else {},
                                        field_schema['group'], '{}{}'.format(path_prefix, name)):
                yield rvals
        else:
            yield (field_schema, fields, '{}{}'.format(path_prefix, name))


def iterate_dict(container, exclude=None, path=None):
    """Iterate over a nested dictionary.

    The dictionary is iterated over in a depth first manner.

    :param container: Dictionary to iterate over
    :param exclude: Optional callable, which is given key and value as
        arguments and may return True to stop iteration of that branch
    :return: (path, key, value) tuple
    """
    if path is None:
        path = []

    for key, value in container.items():
        if callable(exclude) and exclude(key, value):
            continue

        if isinstance(value, collections.Mapping):
            for inner_path, inner_key, inner_value in iterate_dict(value, exclude=exclude, path=path + [key]):
                yield inner_path, inner_key, inner_value

        yield path, key, value
