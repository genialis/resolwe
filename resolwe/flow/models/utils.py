"""Resolwe models utils."""
import copy
import json
import os
import re

import jsonschema

from django.conf import settings
from django.contrib.staticfiles import finders
from django.core.exceptions import ValidationError

from resolwe.flow.utils import dict_dot, iterate_dict, iterate_fields, iterate_schema


class DirtyError(ValidationError):
    """Error raised when required fields missing."""


def validation_schema(name):
    """Return json schema for json validation."""
    schemas = {
        'processor': 'processSchema.json',
        'descriptor': 'descriptorSchema.json',
        'field': 'fieldSchema.json',
        'type': 'typeSchema.json',
    }

    if name not in schemas:
        raise ValueError()

    field_schema_file = finders.find('flow/{}'.format(schemas['field']), all=True)[0]
    with open(field_schema_file, 'r') as fn:
        field_schema = fn.read()

    if name == 'field':
        return json.loads(field_schema.replace('{{PARENT}}', ''))

    schema_file = finders.find('flow/{}'.format(schemas[name]), all=True)[0]
    with open(schema_file, 'r') as fn:
        schema = fn.read()

    return json.loads(schema.replace('{{FIELD}}', field_schema).replace('{{PARENT}}', '/field'))


TYPE_SCHEMA = validation_schema('type')


def validate_schema(instance, schema, test_required=True, path_prefix=None,
                    skip_missing_data=False):
    """Check if DictField values are consistent with our data types.

    Perform basic JSON schema validation and our custom validations:

      * check that required fields are given (if `test_required` is set
        to ``True``)
      * check if ``basic:file:`` and ``list:basic:file`` fields match
        regex given in schema (only if ``validate_regex`` is defined in
        schema for coresponding fields) and exists (only if
        ``path_prefix`` is given)
      * check if directories referenced in ``basic:dir:`` and
        ``list:basic:dir``fields exist (only if ``path_prefix`` is
        given)
      * check that referenced ``Data`` objects (in ``data:<data_type>``
        and  ``list:data:<data_type>`` fields) exists and are of type
        ``<data_type>``
      * check that referenced ``Storage`` objects (in ``basic:json``
        fields) exists

    :param list instance: Instance to be validated
    :param list schema: Schema for validation
    :param bool test_required: Flag for testing if all required fields
        are present. It is usefule if validation is run before ``Data``
        object is finished and there are some field stil missing
        (default: ``False``)
    :param str path_prefix: path prefix used for checking if files and
        directories exist (default: ``None``)
    :param bool skip_missing_data: Don't raise an error if referenced
        ``Data`` object does not exist
    :rtype: None
    :raises ValidationError: if ``instance`` doesn't match schema
        defined in ``schema``

    """
    from .storage import Storage  # Prevent circular import.

    def validate_refs(field):
        """Validate reference paths."""
        for ref_filename in field.get('refs', []):
            ref_path = os.path.join(path_prefix, ref_filename)
            if not os.path.exists(ref_path):
                raise ValidationError("Path referenced in `refs` ({}) does not exist.".format(ref_path))
            if not (os.path.isfile(ref_path) or os.path.isdir(ref_path)):
                raise ValidationError(
                    "Path referenced in `refs` ({}) is neither a file or directory.".format(ref_path))

    def validate_file(field, regex):
        """Validate file name (and check that it exists)."""
        filename = field['file']

        if regex and not re.search(regex, filename):
            raise ValidationError(
                "File name {} does not match regex {}".format(filename, regex))

        if path_prefix:
            path = os.path.join(path_prefix, filename)
            if not os.path.exists(path):
                raise ValidationError("Referenced path ({}) does not exist.".format(path))
            if not os.path.isfile(path):
                raise ValidationError("Referenced path ({}) is not a file.".format(path))

            validate_refs(field)

    def validate_dir(field):
        """Check that dirs and referenced files exists."""
        dirname = field['dir']

        if path_prefix:
            path = os.path.join(path_prefix, dirname)
            if not os.path.exists(path):
                raise ValidationError("Referenced path ({}) does not exist.".format(path))
            if not os.path.isdir(path):
                raise ValidationError("Referenced path ({}) is not a directory.".format(path))

            validate_refs(field)

    def validate_data(data_pk, type_):
        """Check that `Data` objects exist and is of right type."""
        from .data import Data  # prevent circular import

        data_qs = Data.objects.filter(pk=data_pk).values('process__type')
        if not data_qs.exists():
            if skip_missing_data:
                return

            raise ValidationError(
                "Referenced `Data` object does not exist (id:{})".format(data_pk))
        data = data_qs.first()
        if not data['process__type'].startswith(type_):
            raise ValidationError(
                "Data object of type `{}` is required, but type `{}` is given. "
                "(id:{})".format(type_, data['process__type'], data_pk))

    def validate_range(value, interval, name):
        """Check that given value is inside the specified range."""
        if not interval:
            return

        if value < interval[0] or value > interval[1]:
            raise ValidationError(
                "Value of field '{}' is out of range. It should be between {} and {}.".format(
                    name, interval[0], interval[1]
                )
            )

    is_dirty = False
    dirty_fields = []
    for _schema, _fields, _ in iterate_schema(instance, schema):
        name = _schema['name']
        is_required = _schema.get('required', True)

        if test_required and is_required and name not in _fields:
            is_dirty = True
            dirty_fields.append(name)

        if name in _fields:
            field = _fields[name]
            type_ = _schema.get('type', "")

            # Treat None as if the field is missing.
            if not is_required and field is None:
                continue

            try:
                jsonschema.validate([{"type": type_, "value": field}], TYPE_SCHEMA)
            except jsonschema.exceptions.ValidationError as ex:
                raise ValidationError(ex.message)

            choices = [choice['value'] for choice in _schema.get('choices', [])]
            allow_custom_choice = _schema.get('allow_custom_choice', False)
            if choices and not allow_custom_choice and field not in choices:
                raise ValidationError(
                    "Value of field '{}' must match one of predefined choices. "
                    "Current value: {}".format(name, field)
                )

            if type_ == 'basic:file:':
                validate_file(field, _schema.get('validate_regex'))

            elif type_ == 'list:basic:file:':
                for obj in field:
                    validate_file(obj, _schema.get('validate_regex'))

            elif type_ == 'basic:dir:':
                validate_dir(field)

            elif type_ == 'list:basic:dir:':
                for obj in field:
                    validate_dir(obj)

            elif type_ == 'basic:json:' and not Storage.objects.filter(pk=field).exists():
                raise ValidationError(
                    "Referenced `Storage` object does not exist (id:{})".format(field))

            elif type_.startswith('data:'):
                validate_data(field, type_)

            elif type_.startswith('list:data:'):
                for data_id in field:
                    validate_data(data_id, type_[5:])  # remove `list:` from type

            elif type_ == 'basic:integer:' or type_ == 'basic:decimal:':
                validate_range(field, _schema.get('range'), name)

            elif type_ == 'list:basic:integer:' or type_ == 'list:basic:decimal:':
                for obj in field:
                    validate_range(obj, _schema.get('range'), name)

    try:
        # Check that schema definitions exist for all fields
        for _, _ in iterate_fields(instance, schema):
            pass
    except KeyError as ex:
        raise ValidationError(str(ex))

    if is_dirty:
        dirty_fields = ['"{}"'.format(field) for field in dirty_fields]
        raise DirtyError("Required fields {} not given.".format(', '.join(dirty_fields)))


def _hydrate_values(output, output_schema, data):
    """Hydrate basic:file and basic:json values.

    Find fields with basic:file type and assign a full path to the file.
    Find fields with basic:json type and assign a JSON object from storage.

    """
    def hydrate_path(file_name):
        """Hydrate file paths."""
        from resolwe.flow.managers import manager

        class HydratedPath(str):
            """String wrapper, which also stores the original filename."""

            __slots__ = ('data_id', 'file_name')

            def __new__(cls, value=''):
                """Initialize hydrated path."""
                hydrated = str.__new__(cls, value)
                hydrated.data_id = data.id
                hydrated.file_name = file_name
                return hydrated

        return HydratedPath(manager.get_executor().resolve_data_path(data, file_name))

    def hydrate_storage(storage_id):
        """Hydrate storage fields."""
        from .storage import LazyStorageJSON  # Prevent circular import.

        return LazyStorageJSON(pk=storage_id)

    for field_schema, fields in iterate_fields(output, output_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'].startswith('basic:file:'):
                value['file'] = hydrate_path(value['file'])
                value['refs'] = [hydrate_path(ref) for ref in value.get('refs', [])]

            elif field_schema['type'].startswith('list:basic:file:'):
                for obj in value:
                    obj['file'] = hydrate_path(obj['file'])
                    obj['refs'] = [hydrate_path(ref) for ref in obj.get('refs', [])]

            if field_schema['type'].startswith('basic:dir:'):
                value['dir'] = hydrate_path(value['dir'])
                value['refs'] = [hydrate_path(ref) for ref in value.get('refs', [])]

            elif field_schema['type'].startswith('list:basic:dir:'):
                for obj in value:
                    obj['dir'] = hydrate_path(obj['dir'])
                    obj['refs'] = [hydrate_path(ref) for ref in obj.get('refs', [])]

            elif field_schema['type'].startswith('basic:json:'):
                fields[name] = hydrate_storage(value)

            elif field_schema['type'].startswith('list:basic:json:'):
                fields[name] = [hydrate_storage(storage_id) for storage_id in value]


def hydrate_input_references(input_, input_schema, hydrate_values=True):
    """Hydrate ``input_`` with linked data.

    Find fields with complex data:<...> types in ``input_``.
    Assign an output of corresponding data object to those fields.

    """
    from .data import Data  # prevent circular import

    for field_schema, fields in iterate_fields(input_, input_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'].startswith('data:'):
                if value is None:
                    continue

                try:
                    data = Data.objects.get(id=value)
                except Data.DoesNotExist:
                    fields[name] = {}
                    continue

                output = copy.deepcopy(data.output)
                if hydrate_values:
                    _hydrate_values(output, data.process.output_schema, data)
                output["__id"] = data.id
                output["__type"] = data.process.type
                output["__descriptor"] = data.descriptor
                fields[name] = output

            elif field_schema['type'].startswith('list:data:'):
                outputs = []
                for val in value:
                    if val is None:
                        continue

                    try:
                        data = Data.objects.get(id=val)
                    except Data.DoesNotExist:
                        outputs.append({})
                        continue

                    output = copy.deepcopy(data.output)
                    if hydrate_values:
                        _hydrate_values(output, data.process.output_schema, data)

                    output["__id"] = data.id
                    output["__type"] = data.process.type
                    output["__descriptor"] = data.descriptor
                    outputs.append(output)

                fields[name] = outputs


def hydrate_input_uploads(input_, input_schema, hydrate_values=True):
    """Hydrate input basic:upload types with upload location.

    Find basic:upload fields in input.
    Add the upload location for relative paths.

    """
    from resolwe.flow.managers import manager

    files = []
    for field_schema, fields in iterate_fields(input_, input_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'] == 'basic:file:':
                files.append(value)

            elif field_schema['type'] == 'list:basic:file:':
                files.extend(value)

    urlregex = re.compile(r'^(https?|ftp)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]')
    for value in files:
        if 'file_temp' in value:
            if isinstance(value['file_temp'], str):
                # If file_temp not url, hydrate path.
                if not urlregex.search(value['file_temp']):
                    value['file_temp'] = manager.get_executor().resolve_upload_path(value['file_temp'])
            else:
                # Something very strange happened.
                value['file_temp'] = 'Invalid value for file_temp in DB'


def hydrate_size(data, force=False):
    """Add file and dir sizes.

    Add sizes to ``basic:file:``, ``list:basic:file``, ``basic:dir:``
    and ``list:basic:dir:`` fields.

    ``force`` parameter is used to recompute file sizes also on objects
    that already have these values, e.g. in migrations.
    """
    from .data import Data  # prevent circular import

    def get_dir_size(path):
        """Get directory size."""
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                if not os.path.isfile(file_path):  # Skip all "not normal" files (links, ...)
                    continue
                total_size += os.path.getsize(file_path)
        return total_size

    def get_refs_size(obj, obj_path):
        """Caculate size of all references of ``obj``.

        :param dict obj: Data object's output field (of type file/dir).
        :param str obj_path: Path to ``obj``.
        """
        total_size = 0
        for ref in obj.get('refs', []):
            ref_path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), ref)
            if ref_path in obj_path:
                # It is a common case that ``obj['file']`` is also contained in
                # one of obj['ref']. In that case, we need to make sure that it's
                # size is not counted twice:
                continue
            if os.path.isfile(ref_path):
                total_size += os.path.getsize(ref_path)
            elif os.path.isdir(ref_path):
                total_size += get_dir_size(ref_path)

        return total_size

    def add_file_size(obj):
        """Add file size to the basic:file field."""
        if data.status in [Data.STATUS_DONE, Data.STATUS_ERROR] and 'size' in obj and not force:
            return

        path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), obj['file'])
        if not os.path.isfile(path):
            raise ValidationError("Referenced file does not exist ({})".format(path))

        obj['size'] = os.path.getsize(path)
        obj['total_size'] = obj['size'] + get_refs_size(obj, path)

    def add_dir_size(obj):
        """Add directory size to the basic:dir field."""
        if data.status in [Data.STATUS_DONE, Data.STATUS_ERROR] and 'size' in obj and not force:
            return

        path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), obj['dir'])
        if not os.path.isdir(path):
            raise ValidationError("Referenced dir does not exist ({})".format(path))

        obj['size'] = get_dir_size(path)
        obj['total_size'] = obj['size'] + get_refs_size(obj, path)

    data_size = 0
    for field_schema, fields in iterate_fields(data.output, data.process.output_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'].startswith('basic:file:'):
                add_file_size(value)
                data_size += value.get('total_size', 0)
            elif field_schema['type'].startswith('list:basic:file:'):
                for obj in value:
                    add_file_size(obj)
                    data_size += obj.get('total_size', 0)
            elif field_schema['type'].startswith('basic:dir:'):
                add_dir_size(value)
                data_size += value.get('total_size', 0)
            elif field_schema['type'].startswith('list:basic:dir:'):
                for obj in value:
                    add_dir_size(obj)
                    data_size += obj.get('total_size', 0)

    data.size = data_size


def render_descriptor(data):
    """Render data descriptor.

    The rendering is based on descriptor schema and input context.

    :param data: data instance
    :type data: :class:`resolwe.flow.models.Data` or :class:`dict`

    """
    if not data.descriptor_schema:
        return

    # Set default values
    for field_schema, field, path in iterate_schema(data.descriptor, data.descriptor_schema.schema, 'descriptor'):
        if 'default' in field_schema and field_schema['name'] not in field:
            dict_dot(data, path, field_schema['default'])


def render_template(process, template_string, context):
    """Render template using the specified expression engine."""
    from resolwe.flow.managers import manager

    # Get the appropriate expression engine. If none is defined, do not evaluate
    # any expressions.
    expression_engine = process.requirements.get('expression-engine', None)
    if not expression_engine:
        return template_string

    return manager.get_expression_engine(expression_engine).evaluate_block(template_string, context)


def json_path_components(path):
    """Convert JSON path to individual path components.

    :param path: JSON path, which can be either an iterable of path
        components or a dot-separated string
    :return: A list of path components
    """
    if isinstance(path, str):
        path = path.split('.')

    return list(path)


def validate_process_subtype(supertype_name, supertype, subtype_name, subtype):
    """Perform process subtype validation.

    :param supertype_name: Supertype name
    :param supertype: Supertype schema
    :param subtype_name: Subtype name
    :param subtype: Subtype schema
    :return: A list of validation error strings
    """
    errors = []
    for item in supertype:
        # Ensure that the item exists in subtype and has the same schema.
        for subitem in subtype:
            if item['name'] != subitem['name']:
                continue

            for key in set(item.keys()) | set(subitem.keys()):
                if key in ('label', 'description'):
                    # Label and description can differ.
                    continue
                elif key == 'required':
                    # A non-required item can be made required in subtype, but not the
                    # other way around.
                    item_required = item.get('required', True)
                    subitem_required = subitem.get('required', False)

                    if item_required and not subitem_required:
                        errors.append("Field '{}' is marked as required in '{}' and optional in '{}'.".format(
                            item['name'],
                            supertype_name,
                            subtype_name,
                        ))
                elif item.get(key, None) != subitem.get(key, None):
                    errors.append("Schema for field '{}' in type '{}' does not match supertype '{}'.".format(
                        item['name'],
                        subtype_name,
                        supertype_name
                    ))

            break
        else:
            errors.append("Schema for type '{}' is missing supertype '{}' field '{}'.".format(
                subtype_name,
                supertype_name,
                item['name']
            ))

    return errors


def validate_process_types(queryset=None):
    """Perform process type validation.

    :param queryset: Optional process queryset to validate
    :return: A list of validation error strings
    """
    if not queryset:
        from .process import Process
        queryset = Process.objects.all()

    processes = {}
    for process in queryset:
        dict_dot(
            processes,
            process.type.replace(':', '.') + '__schema__',
            process.output_schema
        )

    errors = []
    for path, key, value in iterate_dict(processes, exclude=lambda key, value: key == '__schema__'):
        if '__schema__' not in value:
            continue

        # Validate with any parent types.
        for length in range(len(path), 0, -1):
            parent_type = '.'.join(path[:length] + ['__schema__'])
            try:
                parent_schema = dict_dot(processes, parent_type)
            except KeyError:
                continue

            errors += validate_process_subtype(
                supertype_name=':'.join(path[:length]),
                supertype=parent_schema,
                subtype_name=':'.join(path + [key]),
                subtype=value['__schema__']
            )

    return errors


def fill_with_defaults(process_input, input_schema):
    """Fill empty optional fields in input with default values."""
    for field_schema, fields, path in iterate_schema(process_input, input_schema):
        if 'default' in field_schema and field_schema['name'] not in fields:
            dict_dot(process_input, path, field_schema['default'])
