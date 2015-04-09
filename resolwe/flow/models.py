import datetime

from django.db import models
from django.conf import settings
from django.core.validators import RegexValidator
from jsonfield import JSONField
from django_pgjsonb import JSONField as JSONBField


class BaseModel(models.Model):

    #: URL slug
    slug = models.SlugField(max_length=50, unique=True)

    #: project title
    title = models.CharField(max_length=50)

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True)

    #: modified date and time
    modified = models.DateTimeField(auto_now=True)

    #: created by user
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

    class Meta:
        abstract = True


class Project(BaseModel):

    #: detailed description
    description = models.TextField(blank=True)

    settings = JSONField()

    data = models.ManyToManyField('Data')


class Tool(BaseModel):

    PERSISTENCE_RAW = 'RAW'
    PERSISTENCE_CACHED = 'CAC'
    PERSISTENCE_TEMP = 'TMP'
    PERSISTENCE_CHOICES = (
        (PERSISTENCE_RAW, 'Raw'),
        (PERSISTENCE_CACHED, 'Cached'),
        (PERSISTENCE_TEMP, 'Temp'),
    )

    #: tool version
    version = models.CharField(max_length=50, validators=[
        RegexValidator(
            regex=r'^[0-9]+(\.[0-9]+)*$',
            message='Version must be dot separated integers',
            code='invalid_version'
        )
    ])

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

    static_schema = models.TextField()
    """
    unremovable/static fields schema (default values in form layout **"General"** for :attr:`Data.static`)

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *user*

    """

    run = models.TextField()
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
#     input_data = models.ForeignKey('Data', related_name='input_data', blank=True, null=True, on_delete=models.SET_NULL)

#     #: output data object
#     output_data = models.ForeignKey('Data', related_name='output_data', blank=True, null=True, on_delete=models.SET_NULL)


class Data(BaseModel):

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

    #: cluster location of distributed file system (set by *API*)
    # cluster = mongoengine.StringField()

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

    #: defines the *var form* (changable by *user*)
    template = models.ForeignKey('Template', blank=True, null=True, on_delete=models.PROTECT)

    #: actual values entered in the *input form* and used by the processor
    input = JSONField()

    #: actual *output values* of the processor
    output = JSONField()

    #: actual values entered in the *static form*
    static = JSONField()

    #: actual values entered in the *var form* (ie. any additional information, annotations)
    var = JSONField()


class Template(BaseModel):

    #: template version
    version = models.CharField(max_length=50, validators=[
        RegexValidator(
            regex=r'^[0-9]+(\.[0-9]+)*$',
            message='Version must be dot separated integers',
            code='invalid_version'
        )
    ])

    #: user template schema represented as a JSON object
    schema = JSONField()


class Trigger(BaseModel):

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

    #: corresponding data object
    data = models.ForeignKey('Data')

    #: actual JSON stored
    json = JSONBField()
