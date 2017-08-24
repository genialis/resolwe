"""Reslowe process model."""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import json
import os

import six

from django.conf import settings
from django.contrib.postgres.fields import ArrayField, JSONField
from django.core.exceptions import ValidationError
from django.core.validators import RegexValidator
from django.db import models, transaction

from guardian.shortcuts import assign_perm

from resolwe.flow.expression_engines.exceptions import EvaluationError
from resolwe.flow.utils import dict_dot, get_data_checksum, iterate_fields, iterate_schema

from .base import BaseModel
from .descriptor import DescriptorSchema
from .entity import Entity
from .storage import Storage
from .utils import (
    DirtyError, hydrate_input_references, hydrate_size, render_descriptor, render_template, validate_schema,
)

# Compat between Python 2.7/3.4 and Python 3.5
if not hasattr(json, 'JSONDecodeError'):
    json.JSONDecodeError = ValueError


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

    #: data object is uploading
    STATUS_UPLOADING = 'UP'
    #: data object is being resolved
    STATUS_RESOLVING = 'RE'
    #: data object is waiting
    STATUS_WAITING = 'WT'
    #: data object is processing
    STATUS_PROCESSING = 'PR'
    #: data object is done
    STATUS_DONE = 'OK'
    #: data object is in error state
    STATUS_ERROR = 'ER'
    #: data object is in dirty state
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

    #: process started date and time (set by
    #: :meth:`resolwe.flow.executors.BaseFlowExecutor.run` or its derivatives)
    started = models.DateTimeField(blank=True, null=True, db_index=True)

    #: process finished date date and time (set by
    #: :meth:`resolwe.flow.executors.BaseFlowExecutor.run` or its derivatives)
    finished = models.DateTimeField(blank=True, null=True, db_index=True)

    #: checksum field calculated on inputs
    checksum = models.CharField(max_length=64, db_index=True, validators=[
        RegexValidator(
            regex=r'^[0-9a-f]{64}$',
            message='Checksum is exactly 40 alphanumerics',
            code='invalid_checksum'
        )
    ])

    status = models.CharField(max_length=2, choices=STATUS_CHOICES, default=STATUS_RESOLVING)
    """
    :class:`Data` status

    It can be one of the following:

    - :attr:`STATUS_UPLOADING`
    - :attr:`STATUS_RESOLVING`
    - :attr:`STATUS_WAITING`
    - :attr:`STATUS_PROCESSING`
    - :attr:`STATUS_DONE`
    - :attr:`STATUS_ERROR`
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

    #: actual inputs used by the process
    input = JSONField(default=dict)

    #: actual outputs of the process
    output = JSONField(default=dict)

    #: data descriptor schema
    descriptor_schema = models.ForeignKey('DescriptorSchema', blank=True, null=True, on_delete=models.PROTECT)

    #: actual descriptor
    descriptor = JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    #: track if user set the data name explicitly
    named_by_user = models.BooleanField(default=False)

    #: dependencies between data objects
    parents = models.ManyToManyField(
        'self',
        symmetrical=False, related_name='children'
    )

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
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

                if isinstance(value, six.string_types):
                    file_path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(self.pk), value)
                    if os.path.isfile(file_path):
                        try:
                            with open(file_path) as file_handler:
                                value = json.load(file_handler)
                        except json.JSONDecodeError:
                            with open(file_path) as file_handler:
                                content = file_handler.read()
                                content = content.rstrip()
                                raise ValidationError(
                                    "Value of '{}' must be a valid JSON, current: {}".format(name, content)
                                )

                storage = Storage.objects.create(
                    name='Storage for data id {}'.format(self.pk),
                    contributor=self.contributor,
                    data_id=self.pk,
                    json=value,
                )

                # `value` is copied by value, so `fields[name]` must be changed
                fields[name] = storage.pk

    def save_dependencies(self, instance, schema):
        """Save data: and list:data: references as parents."""
        def add_dependency(value):
            """Add parent Data dependency."""
            try:
                self.parents.add(Data.objects.get(pk=value))  # pylint: disable=no-member
            except Data.DoesNotExist:
                pass

        for field_schema, fields in iterate_fields(instance, schema):
            name = field_schema['name']
            value = fields[name]

            if field_schema.get('type', '').startswith('data:'):
                add_dependency(value)
            elif field_schema.get('type', '').startswith('list:data:'):
                for data in value:
                    add_dependency(data)

    def create_entity(self):
        """Create entity if `flow_collection` is defined in process.

        Following rules applies for adding `Data` object to `Entity`:
        * Only add `Data object` to `Entity` if process has defined
        `flow_collwection` field
        * Add object to existing `Entity`, if all parents that are part
        of it (but not necessary all parents), are part of the same
        `Entity`
        * If parents belong to different `Entities` or do not belong to
        any `Entity`, create new `Entity`

        """
        ds_slug = self.process.flow_collection  # pylint: disable=no-member
        if ds_slug:
            entity_query = Entity.objects.filter(data__in=self.parents.all()).distinct()  # pylint: disable=no-member

            if entity_query.count() == 1:
                entity = entity_query.first()
            else:

                descriptor_schema = DescriptorSchema.objects.filter(slug=ds_slug).latest()
                entity = Entity.objects.create(
                    contributor=self.contributor,
                    descriptor_schema=descriptor_schema,
                    name=self.name,
                    tags=self.tags,
                )

                for permission in list(zip(*entity._meta.permissions))[0]:  # pylint: disable=protected-access
                    assign_perm(permission, entity.contributor, entity)

            entity.data.add(self)

    def save(self, render_name=False, *args, **kwargs):
        """Save the data model."""
        # Generate the descriptor if one is not already set.
        if self.name != self._original_name:
            self.named_by_user = True

        create = self.pk is None
        if create:
            # Default values for INPUT
            input_schema = self.process.input_schema  # pylint: disable=no-member
            for field_schema, fields, path in iterate_schema(self.input, input_schema):
                if 'default' in field_schema and field_schema['name'] not in fields:
                    dict_dot(self.input, path, field_schema['default'])

            if not self.name:
                self._render_name()
            else:
                self.named_by_user = True

            self.checksum = get_data_checksum(
                self.input, self.process.slug, self.process.version)  # pylint: disable=no-member

        elif render_name:
            self._render_name()

        self.save_storage(self.output, self.process.output_schema)  # pylint: disable=no-member

        if self.status != Data.STATUS_ERROR:
            hydrate_size(self)

        if create:
            validate_schema(self.input, self.process.input_schema)  # pylint: disable=no-member

        render_descriptor(self)

        if self.descriptor_schema:
            try:
                validate_schema(self.descriptor, self.descriptor_schema.schema)  # pylint: disable=no-member
                self.descriptor_dirty = False
            except DirtyError:
                self.descriptor_dirty = True
        elif self.descriptor and self.descriptor != {}:
            raise ValueError("`descriptor_schema` must be defined if `descriptor` is given")

        if self.status != Data.STATUS_ERROR:
            path_prefix = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(self.pk))
            output_schema = self.process.output_schema  # pylint: disable=no-member
            if self.status == Data.STATUS_DONE:
                validate_schema(self.output, output_schema, path_prefix=path_prefix)
            else:
                validate_schema(self.output, output_schema, path_prefix=path_prefix,
                                test_required=False)

        with transaction.atomic():
            super(Data, self).save(*args, **kwargs)

            # We can only save dependencies after the data object has been saved. This
            # is why a transaction block is needed and the save method must be called first.
            if create:
                self.save_dependencies(self.input, self.process.input_schema)  # pylint: disable=no-member

        if create:
            self.create_entity()

    def _render_name(self):
        """Render data name.

        The rendering is based on name template (`process.data_name`) and
        input context.

        """
        if not self.process.data_name or self.named_by_user:  # pylint: disable=no-member
            return

        inputs = copy.deepcopy(self.input)
        hydrate_input_references(inputs, self.process.input_schema, hydrate_values=False)  # pylint: disable=no-member
        template_context = inputs

        try:
            name = render_template(
                self.process,
                self.process.data_name,  # pylint: disable=no-member
                template_context
            )
        except EvaluationError:
            name = '?'

        name_max_len = self._meta.get_field('name').max_length
        if len(name) > name_max_len:
            name = name[:(name_max_len - 3)] + '...'

        self.name = name
