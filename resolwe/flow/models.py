"""
===========
Flow Models
===========

Collection Model
*************

Postgres ORM model for the organization of collections.

.. autoclass:: resolwe.flow.models.Case
    :members:


Data model
**********

Postgres ORM model for keeping the data structured.

.. autoclass:: resolwe.flow.models.Data
    :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals
import functools
import json
import jsonschema
import os
import re
import six

from django import template
from django.db import models
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.contrib.postgres.fields import ArrayField, JSONField
from django.contrib.staticfiles import finders

from versionfield import VersionField
from autoslug import AutoSlugField


VERSION_NUMBER_BITS = (8, 10, 14)


class BaseModel(models.Model):

    """Abstract model that ncludes common fields for other models."""

    class Meta:
        """BaseModel Meta options."""
        abstract = True
        unique_together = ('slug', 'version')
        default_permissions = ()

    #: URL slug
    slug = AutoSlugField(populate_from='name', unique_with='version', editable=True, max_length=100)

    #: process version
    version = VersionField(number_bits=VERSION_NUMBER_BITS, default=0)

    #: object name
    name = models.CharField(max_length=100)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True)

    #: modified date and time
    modified = models.DateTimeField(auto_now=True)

    #: user that created the entry
    contributor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

    def __str__(self):
        return self.name


class Process(BaseModel):

    """Postgres model for storing processs."""

    class Meta(BaseModel.Meta):
        """Process Meta options."""
        permissions = (
            ("view_process", "Can view process"),
            ("share_process", "Can share process"),
            ("owner_process", "Is owner of the process"),
        )

    PERSISTENCE_RAW = 'RAW'
    PERSISTENCE_CACHED = 'CAC'
    PERSISTENCE_TEMP = 'TMP'
    PERSISTENCE_CHOICES = (
        (PERSISTENCE_RAW, 'Raw'),
        (PERSISTENCE_CACHED, 'Cached'),
        (PERSISTENCE_TEMP, 'Temp'),
    )

    PRIORITY_HIGH = 'HI'
    PRIORITY_NORMAL = 'NO'
    PRIORITY_CHOICES = (
        (PRIORITY_NORMAL, 'Normal'),
        (PRIORITY_HIGH, 'High'),
    )

    #: data type
    type = models.CharField(max_length=100, validators=[
        RegexValidator(
            regex=r'^data:[a-z0-9:]+:$',
            message='Type may be alphanumerics separated by colon',
            code='invalid_type'
        )
    ])

    #: category
    category = models.CharField(max_length=200, default='other', validators=[
        RegexValidator(
            regex=r'^([a-z0-9]+[:\-])*[a-z0-9]+:$',
            message='Category may be alphanumerics separated by colon',
            code='invalid_category'
        )
    ])

    persistence = models.CharField(max_length=3, choices=PERSISTENCE_CHOICES, default=PERSISTENCE_RAW)
    """
    data PERSISTENCE, cached and temp must be idempotent

    - :attr:`Processor.PERSISTENCE_RAW` / ``'raw'``
    - :attr:`Processor.PERSISTENCE_CACHED` / ``'cached'``
    - :attr:`Processor.PERSISTENCE_TEMP` / ``'temp'``

    """

    priority = models.CharField(max_length=2, choices=PRIORITY_CHOICES, default=PRIORITY_NORMAL)
    """
    data PRIORITY

    - :attr:`Processor.PRIORITY_NORMAL` / ``'normal'``
    - :attr:`Processor.PRIORITY_HIGH` / ``'high'``

    """

    #: detailed description
    description = models.TextField(default='')

    #: template for name of Data object created with Process
    data_name = models.CharField(max_length=200, null=True, blank=True)

    input_schema = JSONField(blank=True, default=list)
    """
    process input schema (describes input parameters, form layout **"Inputs"** for :attr:`Data.input`)

    Handling:

    - schema defined by: *dev*
    - default by: *user*
    - changable by: *none*

    """

    output_schema = JSONField(blank=True, default=list)
    """
    process output schema (describes output JSON, form layout **"Results"** for :attr:`Data.output`)

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *dev*

    Implicitly defined fields (by :meth:`server.management.commands.register` or :meth:`server.tasks.manager`):

    - ``progress`` of type ``basic:float`` (from 0.0 to 1.0)
    - ``proc`` of type ``basic:group`` containing:

      - ``stdout`` of type ``basic:text``
      - ``rc`` of type ``basic:integer``
      - ``task`` of type ``basic:string`` (celery task id)
      - ``worker`` of type ``basic:string`` (celery worker hostname)
      - ``runtime`` of type ``basic:string`` (runtime instance hostname)
      - ``pid`` of type ``basic:integer`` (process ID)

    """

    flow_collection = models.CharField(max_length=100, null=True, blank=True)
    """
    Automatically add Data object created with this processor to a
    special collection representing a data-flow. If all input Data
    objects belong to the same collection, add newly created Data object
    to it, otherwise create a new collection.
    If `DescriptorSchema` object with `type` matching this field
    exists, reference it in the collection's `descriptor_schema` field.

    """

    run = JSONField(default=dict)
    """
    process command and environment description for internal use

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *dev*

    Required definitions:

    - ``engine`` .. engine to run the processor with
    - ``script`` .. script with code to run

    """


def render_template(template_string, context):
    """Render template based on Dango template language."""

    template_headers = [
        '{% load resource_filters %}',
        '{% load process_fields %}',
        '{% load mathfilters %}',
    ]
    return template.Template(''.join(template_headers) + template_string).render(context)


def render_descriptor(data):
    """Render data descriptor.

    The rendering is based on descriptor schema and input context.

    :param data: data instance
    :type data: :obj:`server.models.Data` or :obj:`dict`

    """
    if not data.descriptor_schema or not data.process.input_schema:
        return

    inputs = data.input.copy()
    hydrate_input_references(inputs, data.process.input_schema, hydrate_values=False)
    template_context = template.Context(inputs)

    # Set default values
    for field_schema, _, path in iterate_schema(data.descriptor, data.descriptor_schema.schema, 'descriptor'):
        if 'default' in field_schema:
            tmpl = field_schema['default']
            if field_schema['type'].startswith('list:'):
                tmpl = [render_template(tmp, template_context)
                        if isinstance(tmp, six.string_types) else tmp
                        for tmp in tmpl]
            elif isinstance(tmpl, six.string_types):
                tmpl = render_template(tmpl, template_context)

            dict_dot(data, path, tmpl)


class Data(BaseModel):

    """Postgres model for storing data."""

    class Meta(BaseModel.Meta):
        """Data Meta options."""
        permissions = (
            ("view_data", "Can view data"),
            ("edit_data", "Can edit data"),
            ("share_data", "Can share data"),
            ("download_data", "Can download files from data"),
            ("owner_data", "Is owner of the data"),
        )

    STATUS_UPLOADING = 'UP'
    STATUS_RESOLVING = 'RE'
    STATUS_WAITING = 'WT'
    STATUS_PROCESSING = 'PR'
    STATUS_DONE = 'OK'
    STATUS_ERROR = 'ER'
    STATUS_DIRTY = 'DR'
    STATUS_CHOICES = (
        (STATUS_UPLOADING, 'Uploading'),
        (STATUS_RESOLVING, 'Resolving'),
        (STATUS_WAITING, 'Waiting'),
        (STATUS_PROCESSING, 'Processing'),
        (STATUS_DONE, 'Done'),
        (STATUS_ERROR, 'Error'),
        (STATUS_DIRTY, 'Dirty')
    )

    #: processor started date and time (set by :meth:`server.tasks.manager`)
    started = models.DateTimeField(blank=True, null=True)

    #: processor finished date date and time (set by :meth:`server.tasks.manager`)
    finished = models.DateTimeField(blank=True, null=True)

    #: checksum field calculated on inputs
    checksum = models.CharField(max_length=40, validators=[
        RegexValidator(
            regex=r'^[0-9a-f]{40}$',
            message='Checksum is exactly 40 alphanumerics',
            code='invalid_checksum'
        )
    ], blank=True, null=True)

    status = models.CharField(max_length=2, choices=STATUS_CHOICES, default=STATUS_RESOLVING)
    """
    :class:`Data` status

    - :attr:`Data.STATUS_UPLOADING` / ``'uploading'``
    - :attr:`Data.STATUS_RESOLVING` / ``'resolving'``
    - :attr:`Data.STATUS_WAITING` / ``'waiting'``
    - :attr:`Data.STATUS_PROCESSING` / ``'processing'``
    - :attr:`Data.STATUS_DONE` / ``'done'``
    - :attr:`Data.STATUS_ERROR` / ``'error'``
    """

    #: process used to compute the data object
    process = models.ForeignKey('Process', on_delete=models.PROTECT)

    #: process id
    process_pid = models.PositiveIntegerField(blank=True, null=True)

    #: progress
    process_progress = models.PositiveSmallIntegerField(default=0)

    #: return code
    process_rc = models.PositiveSmallIntegerField(blank=True, null=True)

    #: info log message
    process_info = ArrayField(models.CharField(max_length=255), default=[])

    #: warning log message
    process_warning = ArrayField(models.CharField(max_length=255), default=[])

    #: error log message
    process_error = ArrayField(models.CharField(max_length=255), default=[])

    #: actual inputs used by the processor
    input = JSONField(default=dict)

    #: actual outputs of the processor
    output = JSONField(default=dict)

    #: data descriptor schema
    descriptor_schema = models.ForeignKey('DescriptorSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: actual descriptor
    descriptor = JSONField(default=dict)

    # track if user set the data name explicitly
    named_by_user = models.BooleanField(default=False)

    def __init__(self, *args, **kwargs):
        super(Data, self).__init__(*args, **kwargs)
        self._original_name = self.name

    def save_storage(self, instance, schema):
        """Save basic:json values to a Storage collection."""

        for field_schema, fields in iterate_fields(instance, schema):
            name = field_schema['name']
            value = fields[name]
            if field_schema.get('type', '').startswith('basic:json:'):
                if value and not self.pk:
                    raise ValidationError(
                        'Data object must be `created` before creating `basic:json:` fields')

                if isinstance(value, int):
                    # already in Storage
                    continue

                storage = Storage.objects.create(
                    name='Storage for data id {}'.format(self.pk),
                    contributor=self.contributor,
                    data_id=self.pk,
                    json=value,
                )

                # `value` is copied by value, so `fields[name]` must be changed
                fields[name] = storage.pk

    def save(self, render_name=False, *args, **kwargs):
        # Generate the descriptor if one is not already set.
        if self.name != self._original_name:
            self.named_by_user = True

        create = self.pk is None
        if create:
            # Default values for INPUT
            for field_schema, fields, path in iterate_schema(self.input, self.process.input_schema, ''):
                if 'default' in field_schema and field_schema['name'] not in fields:
                    dict_dot(self.input, path, field_schema['default'])

            if not self.name:
                self._render_name()
            else:
                self.named_by_user = True

        elif render_name:
            self._render_name()

        if not self.descriptor:
            render_descriptor(self)

        self.save_storage(self.output, self.process.output_schema)

        if create:
            validate_schema(self.input, self.process.input_schema)

        if self.descriptor_schema:
            validate_schema(self.descriptor, self.descriptor_schema.schema)
        elif self.descriptor and self.descriptor != {}:
            raise ValueError("`descriptor_schema` must be defined if `descriptor` is given")

        path_prefix = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(self.pk))
        if self.status == Data.STATUS_DONE:
            validate_schema(self.output, self.process.output_schema, path_prefix=path_prefix)
        else:
            validate_schema(self.output, self.process.output_schema, path_prefix=path_prefix,
                            test_required=False)

        super(Data, self).save(*args, **kwargs)

    def _render_name(self):
        """Render data name.

        The rendering is based on name template (`process.data_name`) and
        input context.

        """
        if not self.process.data_name or self.named_by_user:
            return

        inputs = self.input.copy()
        hydrate_input_references(inputs, self.process.input_schema, hydrate_values=False)
        template_context = template.Context(inputs)

        self.name = render_template(self.process.data_name, template_context)


class DescriptorSchema(BaseModel):

    """Postgres model for storing descriptors."""

    class Meta(BaseModel.Meta):
        """DescriptorSchema Meta options."""
        permissions = (
            ("view_descriptorschema", "Can view descriptor schema"),
            ("edit_descriptorschema", "Can edit descriptor schema"),
            ("share_descriptorschema", "Can share descriptor schema"),
            ("owner_descriptorschema", "Is owner of the description schema"),
        )

    #: detailed description
    description = models.TextField(blank=True)

    #: user descriptor schema represented as a JSON object
    schema = JSONField(default=dict)


class Trigger(BaseModel):

    """Postgres model for storing triggers."""

    class Meta(BaseModel.Meta):
        """Data Meta options."""
        permissions = (
            ("view_trigger", "Can view trigger"),
            ("edit_trigger", "Can edit trigger"),
            ("share_trigger", "Can share trigger"),
            ("owner_trigger", "Is owner of the trigger"),
        )

    #: data type of triggering data objects
    type = models.CharField(max_length=100, validators=[
        RegexValidator(
            regex=r'^data:[a-z0-9:]+:$',
            message='Type may be alphanumerics separated by colon',
            code='invalid_type'
        )
    ])

    #: trigger condition
    trigger = models.CharField(max_length=500)

    #: path to where the id is inserted
    trigger_input = models.CharField(max_length=100)

    #: process used
    process = models.ForeignKey('Process', blank=True, null=True, on_delete=models.SET_NULL)

    #: input settings of the processor
    input = JSONField(default=dict)

    #: corresponding collection
    collection = models.ForeignKey('Collection')

    #: does the trigger run on its own
    autorun = models.BooleanField(default=False)


class Storage(BaseModel):

    """Postgres model for storing storages."""

    #: corresponding data object
    data = models.ForeignKey('Data')

    #: actual JSON stored
    json = JSONField()


class LazyStorageJSON(object):

    """Lazy load `json` attribute of `Storage` object."""

    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self._json = None

    def _get_storage(self):
        """Load `json` field from `Storage` object."""
        if self._json is None:
            self._json = Storage.objects.get(**self._kwargs).json

    def __getitem__(self, key):
        self._get_storage()
        return self._json[key]

    def __repr__(self):
        self._get_storage()
        return self._json.__repr__()


class BaseCollection(BaseModel):

    """Template for Postgres model for storing collection."""

    class Meta(BaseModel.Meta):
        """Collection Meta options."""
        abstract = True
        permissions = (
            ("view_collection", "Can view collection"),
            ("edit_collection", "Can edit collection"),
            ("share_collection", "Can share collection"),
            ("download_collection", "Can download files from collection"),
            ("add_collection", "Can add data objects to collection"),
            ("owner_collection", "Is owner of the collection"),
        )

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField(default=dict)

    public_processes = models.ManyToManyField(Process)

    data = models.ManyToManyField(Data)

    #: collection descriptor schema
    descriptor_schema = models.ForeignKey(DescriptorSchema, blank=True, null=True, on_delete=models.PROTECT)

    #: collection descriptor
    descriptor = JSONField(default=dict)


class Collection(BaseCollection):

    """Postgres model for storing collection."""

    pass


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
                yield (rvals if path_prefix is not None else rvals[:2])
        else:
            rvals = (schema_dict[field_id], fields, path)
            yield (rvals if path_prefix is not None else rvals[:2])


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
    field_schema = open(field_schema_file, 'r').read()

    if name == 'field':
        return json.loads(field_schema.replace('{{PARENT}}', ''))

    schema_file = finders.find('flow/{}'.format(schemas[name]), all=True)[0]
    schema = open(schema_file, 'r').read()

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

            if 'refs' in field:
                for refs_filename in field['refs']:
                    refs_path = os.path.join(path_prefix, refs_filename)
                    if not os.path.isfile(refs_path):
                        raise ValidationError(
                            "File referenced in `refs` ({}) does not exist".format(refs_path))

    def validate_data(data_pk, type_):
        """"Check that `Data` objects exist and is of right type."""
        data_qs = Data.objects.filter(pk=data_pk).values('process__type')
        if not data_qs.exists():
            raise ValidationError(
                "Referenced `Data` object does not exist (id:{})".format(data_pk))
        if not data_qs.first()['process__type'].startswith(type_):
            raise ValidationError(
                "Referenced `Data` object is of wrong type (id:{})".format(data_pk))

    for _schema, _fields, _ in iterate_schema(instance, schema):
        name = _schema['name']

        if test_required and _schema.get('required', False) and name not in _fields:
            raise ValidationError("Required field \"{}\" not given.".format(name))

        if name in _fields:
            field = _fields[name]
            type_ = _schema.get('type', "")

            try:
                jsonschema.validate([{"type": type_, "value": field}], TYPE_SCHEMA)
            except jsonschema.exceptions.ValidationError as ex:
                raise ValidationError(ex.message)

            if type_ == 'basic:file:':
                validate_file(field, _schema.get('validate_regex'))

            elif type_ == 'list:basic:file:':
                for obj in field:
                    validate_file(obj, _schema.get('validate_regex'))

            elif type_ == 'basic:json:' and not Storage.objects.filter(pk=field).exists():
                raise ValidationError(
                    "Referenced `Storage` object does not exist (id:{})".format(field))

            elif type_.startswith('data:'):
                validate_data(field, type_)

            elif type_.startswith('list:data:'):
                for data_id in field:
                    validate_data(data_id, type_[5:])  # remove `list:` from type

    for field_schema, fields in iterate_fields(instance, schema):
        pass  # check that schema definitions exist for all fields


def _hydrate_values(output, output_schema, data):
    """Hydrate basic:file and basic:json values.

    Find fields with basic:file type and assign a full path to the file.
    Find fields with basic:json type and assign a JSON object from storage.

    """
    def hydrate_path(file_name):
        id_ = "{}/".format(data.id)  # needs trailing slash
        if id_ in file_name:
            file_name = file_name[file_name.find(id_) + len(id_):]  # remove id from filename

        return os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], id_, file_name)

    def hydrate_storage(storage_id):
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

def hydrate_input_uploads(input_, input_schema, hydrate_values=True):
    """Hydrate input basic:upload types with upload location

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


def hydrate_input_references(input_, input_schema, hydrate_values=True):
    """Hydrate ``input_`` with linked data.

    Find fields with complex data:<...> types in ``input_``.
    Assign an output of corresponding data object to those fields.

    """
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
                output = data.output.copy()
                # static = Data.static.to_python(data.static)
                if hydrate_values:
                    _hydrate_values(output, data.process.output_schema, data)
                    # _hydrate_values(static, data.static_schema, data)
                output["__id"] = data.id
                output["__type"] = data.process.type
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
                    output = data.output.copy()
                    # static = Data.static.to_python(data.static)
                    if hydrate_values:
                        _hydrate_values(output, data.process.output_schema, data)
                        # _hydrate_values(static, data.static_schema, data)

                    output["__id"] = data.id
                    output["__type"] = data.process.type
                    outputs.append(output)

                fields[name] = outputs


def dict_dot(d, k, val=None, default=None):
    """Get or set value using a dot-notation key in a multilevel dict."""
    if val is None and k == '':
        return d

    def set_default(dict_or_model, key, default_value):
        if isinstance(dict_or_model, models.Model):
            if not hasattr(dict_or_model, key):
                setattr(dict_or_model, key, default_value)

            return getattr(dict_or_model, key)
        else:
            return dict_or_model.setdefault(key, default_value)

    def get_item(dict_or_model, key):
        if isinstance(dict_or_model, models.Model):
            return getattr(dict_or_model, key)
        else:
            return dict_or_model[key]

    def set_item(dict_or_model, key, value):
        if isinstance(dict_or_model, models.Model):
            setattr(dict_or_model, key, value)
        else:
            dict_or_model[key] = value

    if val is None and callable(default):
        # Get value, default for missing
        return functools.reduce(lambda a, b: set_default(a, b, default()), k.split('.'), d)

    elif val is None:
        # Get value, error on missing
        return functools.reduce(lambda a, b: get_item(a, b), k.split('.'), d)

    else:
        # Set value
        try:
            k, k_last = k.rsplit('.', 1)
            set_item(dict_dot(d, k, default=dict), k_last, val)
        except ValueError:
            set_item(d, k, val)
        return val
