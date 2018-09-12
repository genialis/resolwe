"""Reslowe process model."""
from django.conf import settings
from django.contrib.postgres.fields import JSONField
from django.core.validators import RegexValidator
from django.db import models

from .base import BaseModel


class Process(BaseModel):
    """Postgres model for storing processes."""

    class Meta(BaseModel.Meta):
        """Process Meta options."""

        permissions = (
            ("view_process", "Can view process"),
            ("share_process", "Can share process"),
            ("owner_process", "Is owner of the process"),
        )

    #: raw persistence
    PERSISTENCE_RAW = 'RAW'
    #: cached persistence
    PERSISTENCE_CACHED = 'CAC'
    #: temp persistence
    PERSISTENCE_TEMP = 'TMP'
    PERSISTENCE_CHOICES = (
        (PERSISTENCE_RAW, 'Raw'),
        (PERSISTENCE_CACHED, 'Cached'),
        (PERSISTENCE_TEMP, 'Temp'),
    )

    SCHEDULING_CLASS_INTERACTIVE = 'IN'
    SCHEDULING_CLASS_BATCH = 'BA'
    SCHEDULING_CLASS_CHOICES = (
        (SCHEDULING_CLASS_INTERACTIVE, "Interactive"),
        (SCHEDULING_CLASS_BATCH, "Batch"),
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
    category = models.CharField(max_length=200, default='Other:', validators=[
        RegexValidator(
            regex=r'^([a-zA-Z0-9]+[:\-])*[a-zA-Z0-9]+:$',
            message='Category may be alphanumerics separated by colon',
            code='invalid_category'
        )
    ])

    persistence = models.CharField(max_length=3, choices=PERSISTENCE_CHOICES, default=PERSISTENCE_RAW)
    """
    Persistence of :class:`~resolwe.flow.models.Data` objects created
    with this process. It can be one of the following:

    - :attr:`PERSISTENCE_RAW`
    - :attr:`PERSISTENCE_CACHED`
    - :attr:`PERSISTENCE_TEMP`

    .. note::

        If persistence is set to ``PERSISTENCE_CACHED`` or
        ``PERSISTENCE_TEMP``, the process must be idempotent.

    """

    #: designates whether this process should be treated as active
    is_active = models.BooleanField('active', default=True)

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

    Implicitly defined fields (by
    :func:`resolwe.flow.management.commands.register` or
    ``resolwe.flow.executors.run.BaseFlowExecutor.run`` or its
    derivatives):

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
    Automatically add :class:`~resolwe.flow.models.Data` object created
    with this process to a special
    :class:`~resolwe.flow.models.Collection` object representing a
    data-flow. If all input ``Data`` objects belong to the same
    collection, add newly created ``Data`` object to it, otherwise
    create a new collection.
    If :class:`~resolwe.flow.models.DescriptorSchema` object with
    ``type`` matching this field exists, reference it in the
    collection's
    :attr:`~resolwe.flow.models.collection.BaseCollection.descriptor_schema`
    field.

    """

    run = JSONField(default=dict)
    """
    process command and environment description for internal use

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *dev*

    """

    requirements = JSONField(default=dict)
    """
    process requirements
    """

    scheduling_class = models.CharField(max_length=2, choices=SCHEDULING_CLASS_CHOICES,
                                        default=SCHEDULING_CLASS_BATCH)
    """
    process scheduling class
    """

    def get_resource_limits(self):
        """Get the core count and memory usage limits for this process.

        :return: A dictionary with the resource limits, containing the
            following keys:

            - ``memory``: Memory usage limit, in MB. Defaults to 4096 if
              not otherwise specified in the resource requirements.
            - ``cores``: Core count limit. Defaults to 1.

        :rtype: dict
        """
        # Get limit defaults and overrides.
        limit_defaults = getattr(settings, 'FLOW_PROCESS_RESOURCE_DEFAULTS', {})
        limit_overrides = getattr(settings, 'FLOW_PROCESS_RESOURCE_OVERRIDES', {})

        limits = {}

        resources = self.requirements.get('resources', {})  # pylint: disable=no-member

        limits['cores'] = int(resources.get('cores', 1))

        max_cores = getattr(settings, 'FLOW_PROCESS_MAX_CORES', None)
        if max_cores:
            limits['cores'] = min(limits['cores'], max_cores)

        memory = limit_overrides.get('memory', {}).get(self.slug, None)
        if memory is None:
            memory = int(resources.get(
                'memory',
                # If no memory resource is configured, check settings.
                limit_defaults.get('memory', 4096)
            ))
        limits['memory'] = memory

        return limits
