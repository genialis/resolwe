"""Reslowe process model."""

from collections import ChainMap
from typing import Optional

from django.conf import settings
from django.contrib.postgres.indexes import GinIndex
from django.core.validators import RegexValidator
from django.db import models

from resolwe.permissions.models import PermissionObject

from .base import BaseModel
from .data import Data


class Process(BaseModel, PermissionObject):
    """Postgres model for storing processes."""

    class Meta(BaseModel.Meta):
        """Process Meta options."""

        permissions = (
            ("view", "Can view process"),
            ("share", "Can share process"),
            ("owner", "Is owner of the process"),
        )

        indexes = [
            models.Index(name="idx_process_name", fields=["name"]),
            models.Index(name="idx_process_slug", fields=["slug"]),
            models.Index(name="idx_process_type", fields=["type"]),
            GinIndex(
                name="idx_process_type_trgm",
                fields=["type"],
                opclasses=["gin_trgm_ops"],
            ),
        ]

    #: raw persistence
    PERSISTENCE_RAW = "RAW"
    #: cached persistence
    PERSISTENCE_CACHED = "CAC"
    #: temp persistence
    PERSISTENCE_TEMP = "TMP"
    PERSISTENCE_CHOICES = (
        (PERSISTENCE_RAW, "Raw"),
        (PERSISTENCE_CACHED, "Cached"),
        (PERSISTENCE_TEMP, "Temp"),
    )

    SCHEDULING_CLASS_INTERACTIVE = "IN"
    SCHEDULING_CLASS_BATCH = "BA"
    SCHEDULING_CLASS_CHOICES = (
        (SCHEDULING_CLASS_INTERACTIVE, "Interactive"),
        (SCHEDULING_CLASS_BATCH, "Batch"),
    )

    #: data type
    type = models.CharField(
        max_length=100,
        validators=[
            RegexValidator(
                regex=r"^data:[a-z0-9:]+:$",
                message="Type may be alphanumerics separated by colon",
                code="invalid_type",
            )
        ],
    )

    #: category
    category = models.CharField(
        max_length=200,
        default="Other:",
        validators=[
            RegexValidator(
                regex=r"^([a-zA-Z0-9]+[:\-])*[a-zA-Z0-9]+:$",
                message="Category may be alphanumerics separated by colon",
                code="invalid_category",
            )
        ],
    )

    persistence = models.CharField(
        max_length=3, choices=PERSISTENCE_CHOICES, default=PERSISTENCE_RAW
    )
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
    is_active = models.BooleanField("active", default=True)

    #: detailed description
    description = models.TextField(default="")

    #: template for name of Data object created with Process
    data_name = models.CharField(max_length=200, null=True, blank=True)

    input_schema = models.JSONField(blank=True, default=list)
    """
    process input schema (describes input parameters, form layout **"Inputs"** for :attr:`Data.input`)

    Handling:

    - schema defined by: *dev*
    - default by: *user*
    - changable by: *none*

    """

    output_schema = models.JSONField(blank=True, default=list)
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

    entity_type = models.CharField(max_length=100, null=True, blank=True)
    """
    Automatically add :class:`~resolwe.flow.models.Data` object created
    with this process to an :class:`~resolwe.flow.models.Entity` object
    representing a data-flow. If all input ``Data`` objects belong to
    the same entity, add newly created ``Data`` object to it, otherwise
    create a new one.
    """

    entity_descriptor_schema = models.CharField(max_length=100, null=True, blank=True)
    """
    Slug of the descriptor schema assigned to the Entity created with
    :attr:`~resolwe.flow.models.Process.entity_type`.
    """

    entity_input = models.CharField(max_length=100, null=True, blank=True)
    """
    Limit the entity selection in
    :attr:`~resolwe.flow.models.Process.entity_type` to a single input.
    """

    entity_always_create = models.BooleanField(default=False)
    """
    Create new entity, regardless of ``entity_input`` or
    ``entity_descriptor_schema`` fields.
    """

    run = models.JSONField(default=dict)
    """
    process command and environment description for internal use

    Handling:

    - schema defined by: *dev*
    - default by: *dev*
    - changable by: *dev*

    """

    requirements = models.JSONField(default=dict)
    """
    process requirements
    """

    scheduling_class = models.CharField(
        max_length=2, choices=SCHEDULING_CLASS_CHOICES, default=SCHEDULING_CLASS_BATCH
    )
    """
    process scheduling class
    """

    def get_resource_limits(self, data: Optional[Data] = None):
        """Get the core count and memory usage limits for this process.

        :return: A dictionary with the resource limits, containing the
            following keys:

            - ``memory``: Memory usage limit, in MB. Defaults to 4096 if
              not otherwise specified in the resource requirements.
            - ``cores``: Core count limit. Defaults to 1.
            - ``storage``: Size (in gibibytes) of temporary volume used for
              processing in kubernetes. Defaults to 200.

        :rtype: dict
        """
        # Known resources.
        resources = ["cores", "memory", "storage"]
        # When no resources are defined, use fallback.
        fallback = {"cores": 1, "memory": 4096, "storage": 200}

        # Prepare requirements from environment.
        environment_settings = getattr(settings, "FLOW_PROCESS_RESOURCE_OVERRIDES", {})
        environment_resources = {
            resource: environment_settings.get(resource, {}).get(self.slug)
            for resource in resources
            if environment_settings.get(resource, {}).get(self.slug)
        }

        # Gather requirements for all resources from all sources.
        # The order of requirements determines their priority.
        resources_map = ChainMap(
            data.process_resources if data is not None else {},
            environment_resources,
            self.requirements.get("resources", {}),
            getattr(settings, "FLOW_PROCESS_RESOURCE_DEFAULTS", {}),
            fallback,
        )

        # Prepare the limits and cap them.
        limits = dict()
        for resource in resources:
            limits[resource] = resources_map[resource]
            cap = getattr(settings, f"FLOW_PROCESS_MAX_{resource.upper()}", None)
            if cap is not None:
                limits[resource] = min(limits[resource], cap)

        return limits
