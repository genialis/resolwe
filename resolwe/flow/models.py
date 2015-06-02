"""
===========
Flow Models
===========

"""
from __future__ import absolute_import, division, print_function, unicode_literals
import json

from django.db import models
from django.conf import settings
from django.core.validators import RegexValidator
from django.contrib.postgres.fields import ArrayField
from django.contrib.staticfiles import finders

from jsonfield import JSONField
from django_pgjsonb import JSONField as JSONBField


class BaseModel(models.Model):

    """Abstract model that ncludes common fields for other models."""

    #: URL slug
    slug = models.SlugField(max_length=50)

    #: tool version
    version = models.PositiveIntegerField(default=0)

    #: object name
    name = models.CharField(max_length=50)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True)

    #: modified date and time
    modified = models.DateTimeField(auto_now=True)

    #: user that created the entry
    contributor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

    class Meta:
        """BaseModel Meta options."""
        abstract = True
        unique_together = ('slug', 'version')


class Project(BaseModel):

    """Postgres model for storing projects."""

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField(default={})

    data = models.ManyToManyField('Data')


class Tool(BaseModel):

    """Postgres model for storing tools."""

    PERSISTENCE_RAW = 'RAW'
    PERSISTENCE_CACHED = 'CAC'
    PERSISTENCE_TEMP = 'TMP'
    PERSISTENCE_CHOICES = (
        (PERSISTENCE_RAW, 'Raw'),
        (PERSISTENCE_CACHED, 'Cached'),
        (PERSISTENCE_TEMP, 'Temp'),
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

    #: detailed description
    description = models.TextField(default='')

    input_schema = models.TextField()
    """
    tool input schema (describes input parameters, form layout **"Inputs"** for :attr:`Data.input`)

    Handling:

    - schema defined by: *dev*
    - default by: *user*
    - changable by: *none*

    """

    output_schema = models.TextField()
    """
    tool output schema (describes output JSON, form layout **"Results"** for :attr:`Data.output`)

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

    adapter = models.TextField()
    """
    tool command and environment description for internal use

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *dev*

    Required definitions:

    - ``runtime`` .. runtime environment to run the processor in
    - ``bash`` .. command or interpreter with code to run

    """


# class Signal(models.Model):
#     """Optional class that we can add in the future if in need of fast pipeline assembly."""

#     #: signal name
#     name = models.SlugField(max_length=50)

#     #: input data object
#     input_data = models.ForeignKey('Data', related_name='input_data',
#                                    blank=True, null=True, on_delete=models.SET_NULL)

#     #: output data object
#     output_data = models.ForeignKey('Data', related_name='output_data',
#                                     blank=True, null=True, on_delete=models.SET_NULL)


class Data(BaseModel):

    """Postgres model for storing data."""

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

    #: processor data type
    type = models.CharField(max_length=100, validators=[
        RegexValidator(
            regex=r'^data:[a-z0-9:]+:$',
            message='Type may be alphanumerics separated by colon',
            code='invalid_data_type'
        )
    ])

    #: checksum field calculated on inputs
    checksum = models.CharField(max_length=40, validators=[
        RegexValidator(
            regex=r'^[0-9a-f]{40}$',
            message='Checksum is exactly 40 alphanumerics',
            code='invalid_checksum'
        )
    ])

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

    persistence = models.CharField(max_length=3, choices=Tool.PERSISTENCE_CHOICES, default=Tool.PERSISTENCE_RAW)
    """
    data importance/PERSISTENCE

    - :attr:`Tool.PERSISTENCE_RAW` / ``'raw'``
    - :attr:`Tool.PERSISTENCE_CACHED` / ``'cached'``
    - :attr:`Tool.PERSISTENCE_TEMP` / ``'temp'``
    """

    #: tool used to compute the data object
    tool = models.ForeignKey('Tool', on_delete=models.PROTECT)

    #: process id
    tool_pid = models.PositiveIntegerField(blank=True, null=True)

    #: progress
    tool_progress = models.PositiveSmallIntegerField(default=0)

    #: return code
    tool_rc = models.PositiveSmallIntegerField(blank=True, null=True)

    #: info log message
    tool_info = ArrayField(models.CharField(max_length=255), default=[])

    #: warning log message
    tool_warning = ArrayField(models.CharField(max_length=255), default=[])

    #: error log message
    tool_error = ArrayField(models.CharField(max_length=255), default=[])

    #: actual inputs used by the processor
    input = JSONField(default={})

    #: actual outputs of the processor
    output = JSONField(default={})

    #: data annotation schema
    annotation_schema = models.ForeignKey('AnnotationSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: actual annotation
    annotation = JSONField(default={})


class AnnotationSchema(BaseModel):

    """Postgres model for storing templates."""

    #: detailed description
    description = models.TextField(blank=True)

    #: user annotation schema represented as a JSON object
    schema = JSONField(default={})


class Trigger(BaseModel):

    """Postgres model for storing triggers."""

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

    #: tool used
    tool = models.ForeignKey('Tool', blank=True, null=True, on_delete=models.SET_NULL)

    #: input settings of the processor
    input = JSONField(default={})

    #: corresponding project
    project = models.ForeignKey('Project')

    #: does the trigger run on its own
    autorun = models.BooleanField(default=False)


class Storage(BaseModel):

    """Postgres model for storing storages."""

    #: corresponding data object
    data = models.ForeignKey('Data')

    #: actual JSON stored
    json = JSONBField()


def iterate_fields(fields, schema):
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
    schema_dict = {val['name']: val for val in schema}
    for field_id, properties in fields.items():
        if 'group' in schema_dict[field_id]:
            for _field_schema, _fields in iterate_fields(properties, schema_dict[field_id]['group']):
                yield (_field_schema, _fields)
        else:
            yield (schema_dict[field_id], fields)


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
        'annotation': 'annotationSchema.json',
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


# def hydrate_input_uploads(input_, input_schema, hydrate_values=True):
#     """Hydrate input basic:upload types with upload location

#     Find basic:upload fields in input.
#     Add the upload location for relative paths.

#     """
#     files = []
#     for field_schema, fields in iterate_fields(input_, input_schema):
#         name = field_schema['name']
#         value = fields[name]
#         if 'type' in field_schema:
#             if field_schema['type'] == 'basic:file:':
#                 files.append(value)

#             elif field_schema['type'] == 'list:basic:file:':
#                 files.extend(value)

#     urlregex = re.compile(r'^(https?|ftp)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]')
#     for value in files:
#         if 'file_temp' in value:
#             if not os.path.isabs(value['file_temp']) and not urlregex.search(value['file_temp']):
#                 value['file_temp'] = os.path.join(settings.RUNTIME['upload_path'], value['file_temp'])


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

                data = Data.objects.get(id=value)
                output = data.output.copy()
                # static = Data.static.to_python(data.static)

                # if hydrate_values:
                #     _hydrate_values(output, data.output_schema, data)
                #     _hydrate_values(static, data.static_schema, data)

                output["_id"] = data.id
                output["_type"] = data.type
                fields[name] = output

            elif field_schema['type'].startswith('list:data:'):
                outputs = []
                for val in value:
                    # if re.match('^[0-9a-fA-F]{24}$', str(val)) is None:
                    #     print "ERROR: data:<...> value in {}, type \"{}\" not ObjectId but {}.".format(
                    #         name, field_schema['type'], val)

                    data = Data.objects.get(id=val)
                    output = data.output.copy()
                    # static = Data.static.to_python(data.static)

                    # if hydrate_values:
                    #     _hydrate_values(output, data.output_schema, data)
                    #     _hydrate_values(static, data.static_schema, data)

                    output["_id"] = data.id
                    output["_type"] = data.type
                    outputs.append(output)

                fields[name] = outputs


def dict_dot(d, k, val=None, default=None):
    """Get or set value using a dot-notation key in a multilevel dict."""
    if val is None and k == '':
        return d

    if val is None and callable(default):
        # Get value, default for missing
        # Ugly, but works for model.Data objects as well as dicts
        # Does the same as:
        # return reduce(lambda a, b: a.setdefault(b, default()), k.split('.'), d)
        return reduce(lambda a, b: a.__setitem__(b, a[b] if b in a else default()) or a[b], k.split('.'), d)

    elif val is None:
        # Get value, error on missing
        return reduce(lambda a, b: a[b], k.split('.'), d)

    else:
        # Set value
        try:
            k, k_last = k.rsplit('.', 1)
            dict_dot(d, k, default=dict)[k_last] = val
        except ValueError:
            d[k] = val
        return val
