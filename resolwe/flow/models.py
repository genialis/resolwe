"""
===========
Flow Models
===========

"""
import json
import os

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
    slug = models.SlugField(max_length=50, unique=True)

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


class Project(BaseModel):

    """Postgres model for storing projects."""

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField()

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

    #: tool version
    version = models.PositiveIntegerField()

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
    description = models.TextField(blank=True)

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
    started = models.DateTimeField()

    #: processor finished date date and time (set by :meth:`server.tasks.manager`)
    finished = models.DateTimeField()

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
    tool_pid = models.PositiveSmallIntegerField()

    #: progress
    tool_progress = models.PositiveSmallIntegerField()

    #: output file to log stdout
    tool_stdout = models.CharField(max_length=255)

    #: return code
    tool_rc = models.PositiveSmallIntegerField()

    #: info log message
    tool_info = ArrayField(models.CharField(max_length=255))

    #: warning log message
    tool_warning = ArrayField(models.CharField(max_length=255))

    #: error log message
    tool_error = ArrayField(models.CharField(max_length=255))

    #: actual inputs used by the processor
    input = JSONField()

    #: actual outputs of the processor
    output = JSONField()

    #: data annotation schema
    annotation_schema = models.ForeignKey('AnnotationSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: actual annotation
    annotation = JSONField()


class AnnotationSchema(BaseModel):

    """Postgres model for storing templates."""

    #: annotation schema version
    version = models.PositiveIntegerField()

    #: detailed description
    description = models.TextField(blank=True)

    #: user annotation schema represented as a JSON object
    schema = JSONField()


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
    input = JSONField()

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
    for field_id, properties in fields.iteritems():
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
