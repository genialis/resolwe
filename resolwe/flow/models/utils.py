"""Resolwe models utils."""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import json
import os
import re

import jsonschema
import six

from django.conf import settings
from django.contrib.staticfiles import finders
from django.core.exceptions import ValidationError

from resolwe.flow.utils import dict_dot, iterate_fields, iterate_schema

# TODO: Python 3.5+ imports modules in a different (lazy) way, so when
#       Python 3.4 support is dropped, data module can be imported as:
#
#           from . import data as data_model
#
#       Instead of importing `Data` class in functions, it can be used
#       as:
#
#           data_model.Data
from .storage import LazyStorageJSON, Storage


class DirtyError(ValidationError):
    """Error raised when required fields missing."""


def validation_schema(name):
    """Return json schema for json validation."""
    schemas = {
        'processor': 'processorSchema.json',
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


def validate_schema(instance, schema, test_required=True, path_prefix=None):
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
    :rtype: None
    :raises ValidationError: if ``instance`` doesn't match schema
        defined in ``schema``

    """
    def validate_refs(field):
        """Validate reference paths."""
        if 'refs' in field:
            for refs_filename in field['refs']:
                refs_path = os.path.join(path_prefix, refs_filename)
                if not (os.path.isfile(refs_path) or os.path.isdir(refs_path)):
                    raise ValidationError(
                        "File referenced in `refs` ({}) does not exist".format(refs_path))

    def validate_file(field, regex):
        """Validate file name (and check that it exists)."""
        filename = field['file']

        if regex and not re.search(regex, filename):
            raise ValidationError(
                "File name {} does not match regex {}".format(filename, regex))

        if path_prefix:
            path = os.path.join(path_prefix, filename)
            if not os.path.isfile(path):
                raise ValidationError("Referenced file ({}) does not exist".format(path))

            validate_refs(field)

    def validate_dir(field):
        """Check that dirs and referenced files exists."""
        dirname = field['dir']

        if path_prefix:
            path = os.path.join(path_prefix, dirname)
            if not os.path.isdir(path):
                raise ValidationError("Referenced dir ({}) does not exist".format(path))

            validate_refs(field)

    def validate_data(data_pk, type_):
        """Check that `Data` objects exist and is of right type."""
        from .data import Data  # prevent circular import

        data_qs = Data.objects.filter(pk=data_pk).values('process__type')
        if not data_qs.exists():
            raise ValidationError(
                "Referenced `Data` object does not exist (id:{})".format(data_pk))
        data = data_qs.first()
        if not data['process__type'].startswith(type_):
            raise ValidationError(
                "Data object of type `{}` is required, but type `{}` is given. "
                "(id:{})".format(type_, data['process__type'], data_pk))

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
        id_ = "{}/".format(data.id)  # needs trailing slash
        if id_ in file_name:
            file_name = file_name[file_name.find(id_) + len(id_):]  # remove id from filename

        return os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], id_, file_name)

    def hydrate_storage(storage_id):
        """Hydrate storage fields."""
        return LazyStorageJSON(pk=storage_id)

    for field_schema, fields in iterate_fields(output, output_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'].startswith('basic:file:'):
                value['file'] = hydrate_path(value['file'])

            elif field_schema['type'].startswith('list:basic:file:'):
                for obj in value:
                    obj['file'] = hydrate_path(obj['file'])

            if field_schema['type'].startswith('basic:dir:'):
                value['dir'] = hydrate_path(value['dir'])

            elif field_schema['type'].startswith('list:basic:dir:'):
                for obj in value:
                    obj['dir'] = hydrate_path(obj['dir'])

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
                # if re.match('^[0-9a-fA-F]{24}$', str(value)) is None:
                #     print "ERROR: data:<...> value in field \"{}\", type \"{}\" not ObjectId but {}.".format(
                #         name, field_schema['type'], value)
                if value is None:
                    continue

                data = Data.objects.get(id=value)
                output = copy.deepcopy(data.output)
                # static = Data.static.to_python(data.static)
                if hydrate_values:
                    _hydrate_values(output, data.process.output_schema, data)
                    # _hydrate_values(static, data.static_schema, data)
                output["__id"] = data.id
                output["__type"] = data.process.type
                output["__descriptor"] = data.descriptor
                fields[name] = output

            elif field_schema['type'].startswith('list:data:'):
                outputs = []
                for val in value:
                    # if re.match('^[0-9a-fA-F]{24}$', str(val)) is None:
                    #     print "ERROR: data:<...> value in {}, type \"{}\" not ObjectId but {}.".format(
                    #         name, field_schema['type'], val)
                    if val is None:
                        continue

                    data = Data.objects.get(id=val)
                    output = copy.deepcopy(data.output)
                    # static = Data.static.to_python(data.static)
                    if hydrate_values:
                        _hydrate_values(output, data.process.output_schema, data)
                        # _hydrate_values(static, data.static_schema, data)

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
            if isinstance(value['file_temp'], six.string_types):
                # If file_temp not url, nor absolute path: hydrate path
                if not os.path.isabs(value['file_temp']) and not urlregex.search(value['file_temp']):
                    value['file_temp'] = os.path.join(settings.FLOW_EXECUTOR['UPLOAD_DIR'], value['file_temp'])
            else:
                # Something very strange happened
                value['file_temp'] = 'Invalid value for file_temp in DB'


def hydrate_size(data):
    """Add file and dir sizes.

    Add sizes to ``basic:file:``, ``list:basic:file``, ``basic:dir:``
    and ``list:basic:dir:`` fields.

    """
    from .data import Data  # prevent circular import

    def add_file_size(obj):
        """Add file size to the basic:file field."""
        if data.status in [Data.STATUS_DONE, Data.STATUS_ERROR] and 'size' in obj:
            return

        path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), obj['file'])
        if not os.path.isfile(path):
            raise ValidationError("Referenced file does not exist ({})".format(path))

        obj['size'] = os.path.getsize(path)

    def get_dir_size(path):
        """Get directory size."""
        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                total_size += os.path.getsize(file_path)
        return total_size

    def add_dir_size(obj):
        """Add directory size to the basic:dir field."""
        if data.status in [Data.STATUS_DONE, Data.STATUS_ERROR] and 'size' in obj:
            return

        path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), obj['dir'])
        if not os.path.isdir(path):
            raise ValidationError("Referenced dir does not exist ({})".format(path))

        obj['size'] = get_dir_size(path)

    for field_schema, fields in iterate_fields(data.output, data.process.output_schema):
        name = field_schema['name']
        value = fields[name]
        if 'type' in field_schema:
            if field_schema['type'].startswith('basic:file:'):
                add_file_size(value)
            elif field_schema['type'].startswith('list:basic:file:'):
                for obj in value:
                    add_file_size(obj)
            elif field_schema['type'].startswith('basic:dir:'):
                add_dir_size(value)
            elif field_schema['type'].startswith('list:basic:dir:'):
                for obj in value:
                    add_dir_size(obj)


def render_descriptor(data):
    """Render data descriptor.

    The rendering is based on descriptor schema and input context.

    :param data: data instance
    :type data: :class:`resolwe.flow.models.Data` or :class:`dict`

    """
    if not data.descriptor_schema:
        return

    inputs = copy.deepcopy(data.input)
    if data.process.input_schema:
        hydrate_input_references(inputs, data.process.input_schema, hydrate_values=False)
    template_context = inputs

    # Set default values
    for field_schema, field, path in iterate_schema(data.descriptor, data.descriptor_schema.schema, 'descriptor'):
        if 'default' in field_schema and field_schema['name'] not in field:
            tmpl = field_schema['default']
            if field_schema['type'].startswith('list:'):
                tmpl = [render_template(data.process, tmp, template_context)
                        if isinstance(tmp, six.string_types) else tmp
                        for tmp in tmpl]
            elif isinstance(tmpl, six.string_types):
                tmpl = render_template(data.process, tmpl, template_context)

            dict_dot(data, path, tmpl)


def render_template(process, template_string, context):
    """Render template using the specified expression engine."""
    from resolwe.flow.managers import manager

    # Get the appropriate expression engine. If none is defined, do not evaluate
    # any expressions.
    expression_engine = process.requirements.get('expression-engine', None)
    if not expression_engine:
        return template_string

    return manager.get_expression_engine(expression_engine).evaluate_block(template_string, context)
