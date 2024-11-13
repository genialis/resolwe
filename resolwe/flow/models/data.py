"""Reslowe process model."""

import copy
import enum
import json
import logging
from typing import Any, Optional, Union

import jsonschema
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.validators import RegexValidator
from django.db import models, transaction

from resolwe.flow.expression_engines.exceptions import EvaluationError
from resolwe.flow.models.base import BaseQuerySet
from resolwe.flow.models.utils import (
    DirtyError,
    fill_with_defaults,
    validate_schema,
    validation_schema,
)
from resolwe.flow.utils import dict_dot, get_data_checksum, iterate_fields
from resolwe.observers.consumers import BackgroundTaskType
from resolwe.observers.decorators import move_to_container
from resolwe.observers.models import BackgroundTask
from resolwe.observers.utils import start_background_task
from resolwe.permissions.models import PermissionObject
from resolwe.permissions.utils import assign_contributor_permissions, copy_permissions

from .base import BaseModel
from .entity import Entity, EntityQuerySet
from .history_manager import HistoryMixin
from .secret import Secret
from .storage import Storage
from .utils import (
    hydrate_input_references,
    hydrate_size,
    render_descriptor,
    render_template,
)
from .worker import Worker

# Compatibility for Python < 3.5.
if not hasattr(json, "JSONDecodeError"):
    json.JSONDecodeError = ValueError

logger = logging.getLogger(__name__)


@enum.unique
class HandleEntityOperation(enum.Enum):
    """Constants for entity handling."""

    CREATE = "CREATE"
    ADD = "ADD"
    PASS = "PASS"


class DataQuerySet(BaseQuerySet):
    """Query set for Data objects."""

    @staticmethod
    def _handle_entity(obj: "Data"):
        """Create entity if `entity.type` is defined in process.

        Following rules applies for adding `Data` object to `Entity`:
        * Only add `Data object` to `Entity` if process has defined
        `entity.type` field
        * Create new entity if parents do not belong to any `Entity`
        * Add object to existing `Entity`, if all parents that are part
        of it (but not necessary all parents), are part of the same
        `Entity`
        * If parents belong to different `Entities` don't do anything

        """
        entity_type = obj.process.entity_type
        entity_input = obj.process.entity_input
        entity_always_create = obj.process.entity_always_create
        operation = HandleEntityOperation.PASS

        if entity_type:
            data_filter: dict[str, Union[list, int]] = {}
            if entity_input:
                input_id = dict_dot(obj.input, entity_input, default=lambda: None)
                if input_id is None:
                    logger.warning("Skipping creation of entity due to missing input.")
                    return
                if isinstance(input_id, int):
                    data_filter["data__pk"] = input_id
                elif isinstance(input_id, list):
                    data_filter["data__pk__in"] = input_id
                else:
                    raise ValueError(
                        "Cannot create entity due to invalid value of field {}.".format(
                            entity_input
                        )
                    )
            else:
                data_filter["data__in"] = obj.parents.all()

            entity_query = Entity.objects.filter(
                type=entity_type, **data_filter
            ).distinct()
            entity_count = entity_query.count()

            if entity_count == 0 or entity_always_create:
                entity = Entity.objects.create(
                    contributor=obj.contributor,
                    type=entity_type,
                    name=obj.name,
                    tags=obj.tags,
                )
                assign_contributor_permissions(entity)
                obj.move_to_entity(entity)
                operation = HandleEntityOperation.CREATE

            elif entity_count == 1:
                entity = entity_query.first()
                operation = HandleEntityOperation.ADD
                obj.move_to_entity(entity)

            else:
                logger.info(
                    "Skipping creation of entity due to multiple entities found."
                )

            return operation

    @staticmethod
    def _handle_collection(obj: "Data", entity_operation: HandleEntityOperation):
        """Correctly assign Collection to Data and it's Entity.

        There are 2 x 4 possible scenarios how to handle collection
        assignment. One dimension in "decision matrix" is Data.collection:

            1.x Data.collection = None
            2.x Data.collection != None

        Second dimension is about Data.entity:

            x.1 Data.entity is None
            x.2 Data.entity was just created
            x.3 Data.entity already exists and Data.entity.collection = None
            x.4 Data.entity already exists and Data.entity.collection != None
        """
        # 1.2 and 1.3 require no action.

        # 1.1 and 2.1:
        if not obj.entity:
            return
        if entity_operation == HandleEntityOperation.ADD and obj.collection:
            # 2.3
            if not obj.entity.collection:
                raise ValueError(
                    "Created Data has collection {} assigned, but it is added to entity {} that is not "
                    "inside this collection.".format(obj.collection, obj.entity)
                )
            # 2.4
            assert obj.collection == obj.entity.collection

        # 1.4
        if not obj.collection and obj.entity and obj.entity.collection:
            obj.move_to_collection(obj.entity.collection)
        # 2.2
        if entity_operation == HandleEntityOperation.CREATE and obj.collection:
            obj.entity.move_to_collection(obj.collection)

    @transaction.atomic
    def create(self, subprocess_parent=None, **kwargs):
        """Create new object with the given kwargs."""
        obj = super().create(**kwargs)

        # Data dependencies
        obj.save_dependencies(obj.input, obj.process.input_schema)
        if subprocess_parent:
            DataDependency.objects.create(
                parent=subprocess_parent,
                child=obj,
                kind=DataDependency.KIND_SUBPROCESS,
            )
            # Data was from a workflow / spawned process
            if not obj.in_container():
                copy_permissions(subprocess_parent, obj)

        # Entity, Collection assignment
        entity_operation = self._handle_entity(obj)
        self._handle_collection(obj, entity_operation=entity_operation)

        # Assign contributor permission only if Data is not in the container.
        if not obj.in_container():
            assign_contributor_permissions(obj)

        return obj

    def duplicate(self, request_user) -> BackgroundTask:
        """Duplicate (make a copy) ``Data`` objects in the background."""
        task_data = {
            "data_ids": list(self.values_list("pk", flat=True)),
            "inherit_entity": True,
            "inherit_collection": True,
        }
        return start_background_task(
            BackgroundTaskType.DUPLICATE_DATA, "Duplicate data", task_data, request_user
        )

    def delete_background(self, request_user):
        """Delete the ``Data`` objects in the background."""
        task_data = {
            "object_ids": list(self.values_list("pk", flat=True)),
            "content_type_id": ContentType.objects.get_for_model(self.model).pk,
        }
        return start_background_task(
            BackgroundTaskType.DELETE, "Delete data", task_data, request_user
        )

    def move_to_collection(self, destination_collection, request_user):
        """Move data objects to destination collection in the background.

        Note that this method will also copy tags and permissions
        of the destination collection to the data objects.
        """
        task_data = {
            "target_id": destination_collection.pk,
            "data_ids": list(self.values_list("pk", flat=True)),
            "entity_ids": [],
            "request_user_id": request_user.pk,
        }
        return start_background_task(
            BackgroundTaskType.MOVE, "Delete data", task_data, request_user
        )

    def annotate_sample_path(self, path, annotation_name, value_to_label=False):
        """Add annotation to the Entity QuerySet.

        The annotation is the value of the field with the given name (in the
        given group).

        :attr group_name: the name of the group annotation field belongs to.
        :attr field_name: the name of the annotation field.
        :attr annotation_name: the name under which annotation will be stored.
            When empty the name f"{group_name}_{field_name}" will be used.
        :attr value_to_label: optionally annotate with label instead of the
            value. Only applicable when the vocabulary on the field is given.
        """
        annotation_data = EntityQuerySet._prepare_annotation_data(
            path,
            annotation_name,
            value_to_label,
            outerref_entity_pk_path="entity_id",
        )
        return self.annotate(**annotation_data)


class Data(HistoryMixin, PermissionObject, BaseModel):
    """Postgres model for storing data."""

    class Meta(BaseModel.Meta):
        """Data Meta options."""

        permissions = (
            ("view", "Can view data"),
            ("edit", "Can edit data"),
            ("share", "Can share data"),
            ("owner", "Is owner of the data"),
        )

        indexes = [
            models.Index(name="idx_data_name", fields=["name"]),
            GinIndex(
                name="idx_data_name_trgm", fields=["name"], opclasses=["gin_trgm_ops"]
            ),
            models.Index(name="idx_data_slug", fields=["slug"]),
            models.Index(name="idx_data_status", fields=["status"]),
            GinIndex(name="idx_data_tags", fields=["tags"]),
            GinIndex(name="idx_data_search", fields=["search"]),
        ]

    #: data object is uploading
    STATUS_UPLOADING = "UP"
    #: data object is being resolved
    STATUS_RESOLVING = "RE"
    #: data object is waiting
    STATUS_WAITING = "WT"
    #: data object is preparing
    STATUS_PREPARING = "PP"
    #: data object is processing
    STATUS_PROCESSING = "PR"
    #: data object is done
    STATUS_DONE = "OK"
    #: data object is in error state
    STATUS_ERROR = "ER"
    #: data object is in dirty state
    STATUS_DIRTY = "DR"
    # Assumption (in listener): ordered from least to most problematic.
    STATUS_CHOICES = (
        (STATUS_UPLOADING, "Uploading"),
        (STATUS_RESOLVING, "Resolving"),
        (STATUS_WAITING, "Waiting"),
        (STATUS_PREPARING, "Preparing"),
        (STATUS_PROCESSING, "Processing"),
        (STATUS_DONE, "Done"),
        (STATUS_ERROR, "Error"),
        (STATUS_DIRTY, "Dirty"),
    )

    #: manager
    objects = DataQuerySet.as_manager()

    #: date and time when process was dispatched to the scheduling system
    #: (set by``resolwe.flow.managers.dispatcher.Manager.run``
    scheduled = models.DateTimeField(blank=True, null=True, db_index=True)

    #: process started date and time (set by
    #: ``resolwe.flow.executors.run.BaseFlowExecutor.run`` or its derivatives)
    started = models.DateTimeField(blank=True, null=True, db_index=True)

    #: process finished date date and time (set by
    #: ``resolwe.flow.executors.run.BaseFlowExecutor.run`` or its derivatives)
    finished = models.DateTimeField(blank=True, null=True, db_index=True)

    #: duplication date and time
    duplicated = models.DateTimeField(blank=True, null=True)

    #: checksum field calculated on inputs
    checksum = models.CharField(
        max_length=64,
        db_index=True,
        validators=[
            RegexValidator(
                regex=r"^[0-9a-f]{64}$",
                message="Checksum is exactly 40 alphanumerics",
                code="invalid_checksum",
            )
        ],
    )

    status = models.CharField(
        max_length=2, choices=STATUS_CHOICES, default=STATUS_RESOLVING
    )
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
    process = models.ForeignKey("Process", on_delete=models.PROTECT)

    #: process id
    process_pid = models.PositiveIntegerField(blank=True, null=True)

    #: progress
    process_progress = models.PositiveSmallIntegerField(default=0)

    #: return code
    process_rc = models.SmallIntegerField(blank=True, null=True)

    #: info log message
    process_info = ArrayField(models.CharField(max_length=255), default=list)

    #: warning log message
    process_warning = ArrayField(models.CharField(max_length=255), default=list)

    #: error log message
    process_error = ArrayField(models.CharField(max_length=255), default=list)

    #: actual inputs used by the process
    input = models.JSONField(default=dict)

    #: actual outputs of the process
    output = models.JSONField(default=dict)

    #: total size of data's outputs in bytes
    size = models.BigIntegerField()

    #: data descriptor schema
    descriptor_schema = models.ForeignKey(
        "DescriptorSchema", blank=True, null=True, on_delete=models.PROTECT
    )

    #: actual descriptor
    descriptor = models.JSONField(default=dict)

    #: indicate whether `descriptor` doesn't match `descriptor_schema` (is dirty)
    descriptor_dirty = models.BooleanField(default=False)

    #: track if user set the data name explicitly
    named_by_user = models.BooleanField(default=False)

    #: dependencies between data objects
    parents = models.ManyToManyField(
        "self", through="DataDependency", symmetrical=False, related_name="children"
    )

    #: tags for categorizing objects
    tags = ArrayField(models.CharField(max_length=255), default=list)

    #: actual allocated memory
    process_memory = models.PositiveIntegerField(default=0)

    #: actual allocated cores
    process_cores = models.PositiveSmallIntegerField(default=0)

    #: data location
    location = models.ForeignKey(
        "storage.FileStorage",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        related_name="data",
    )

    #: entity
    entity = models.ForeignKey(
        "Entity", blank=True, null=True, on_delete=models.CASCADE, related_name="data"
    )

    #: collection
    collection = models.ForeignKey(
        "Collection",
        blank=True,
        null=True,
        on_delete=models.CASCADE,
        related_name="data",
    )

    #: field used for full-text search
    search = SearchVectorField(null=True)

    #: process requirements overrides
    process_resources = models.JSONField(default=dict)

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super().__init__(*args, **kwargs)
        self._original_name = self.name
        self._original_output = self.output

    def resolve_secrets(self):
        """Retrieve handles for all basic:secret: fields on input.

        The process must have the ``secrets`` resource requirement
        specified in order to access any secrets. Otherwise this method
        will raise a ``PermissionDenied`` exception.

        :return: A dictionary of secrets where key is the secret handle
            and value is the secret value.
        """
        secrets = {}
        for field_schema, fields in iterate_fields(
            self.input, self.process.input_schema
        ):
            if not field_schema.get("type", "").startswith("basic:secret:"):
                continue

            name = field_schema["name"]
            value = fields[name]
            try:
                handle = value["handle"]
            except KeyError:
                continue

            try:
                secrets[handle] = Secret.objects.get_secret(
                    handle, contributor=self.contributor
                )
            except Secret.DoesNotExist:
                raise PermissionDenied(
                    "Access to secret not allowed or secret does not exist"
                )

        # If the process does not not have the right requirements it is not
        # allowed to access any secrets.
        allowed = self.process.requirements.get("resources", {}).get("secrets", False)
        if secrets and not allowed:
            raise PermissionDenied(
                "Process '{}' has secret inputs, but no permission to see secrets".format(
                    self.process.slug
                )
            )

        return secrets

    def save_dependencies(self, instance, schema):
        """Save data: and list:data: references as parents."""

        def add_dependency(value):
            """Add parent Data dependency."""
            try:
                DataDependency.objects.update_or_create(
                    parent=Data.objects.get(pk=value),
                    child=self,
                    defaults={"kind": DataDependency.KIND_IO},
                )
            except Data.DoesNotExist:
                pass

        for field_schema, fields in iterate_fields(instance, schema):
            name = field_schema["name"]
            value = fields[name]

            if field_schema.get("type", "").startswith("data:"):
                add_dependency(value)
            elif field_schema.get("type", "").startswith("list:data:"):
                for data in value:
                    add_dependency(data)

    def save(self, render_name=False, *args, **kwargs):
        """Save the data model."""
        if self.name != self._original_name:
            self.named_by_user = True

        try:
            jsonschema.validate(
                self.process_resources, validation_schema("process_resources")
            )
        except jsonschema.exceptions.ValidationError as exception:
            # Re-raise as Django ValidationError
            raise ValidationError(exception.message)

        create = self.pk is None
        if create:
            fill_with_defaults(self.input, self.process.input_schema)

            if not self.name:
                self._render_name()
            else:
                self.named_by_user = True

            self.checksum = get_data_checksum(
                self.input, self.process.slug, self.process.version
            )

            validate_schema(self.input, self.process.input_schema)

            hydrate_size(self)
            # If only specified fields are updated (e.g. in executor), size needs to be added
            if "update_fields" in kwargs:
                kwargs["update_fields"].append("size")

        elif render_name:
            self._render_name()

        render_descriptor(self)

        if self.descriptor_schema:
            try:
                validate_schema(self.descriptor, self.descriptor_schema.schema)
                self.descriptor_dirty = False
            except DirtyError:
                self.descriptor_dirty = True
        elif self.descriptor and self.descriptor != {}:
            raise ValueError(
                "`descriptor_schema` must be defined if `descriptor` is given"
            )

        with transaction.atomic():
            self._perform_save(*args, **kwargs)

        self._original_output = self.output

    def _perform_save(self, *args, **kwargs):
        """Save the data model."""
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        """Delete the data model."""
        if hasattr(self, "worker"):
            if self.worker.status not in Worker.FINAL_STATUSES:
                self.worker.terminate()
        # Store ids in memory as relations are also deleted with the Data object.
        storage_ids = list(self.storages.values_list("pk", flat=True))
        super().delete(*args, **kwargs)

        Storage.objects.filter(pk__in=storage_ids, data=None).delete()

    def delete_background(self):
        """Delete the object in the background."""
        task_data = {
            "object_ids": [self.pk],
            "content_type_id": ContentType.objects.get_for_model(self).pk,
        }
        return start_background_task(
            BackgroundTaskType.DELETE, "Delete data", task_data, self.contributor
        )

    def is_duplicate(self):
        """Return True if data object is a duplicate."""
        return bool(self.duplicated)

    def duplicate(self, request_user) -> BackgroundTask:
        """Duplicate (make a copy) object in the background."""
        task_data = {
            "data_ids": [self.pk],
            "inherit_entity": True,
            "inherit_collection": True,
        }
        return start_background_task(
            BackgroundTaskType.DUPLICATE_DATA, "Duplicate data", task_data, request_user
        )

    @move_to_container
    def move_to_collection(self, collection):
        """Move data object to collection."""
        self.validate_change_collection(collection)
        self.collection = collection
        self.permission_group = collection.permission_group
        if collection:
            self.tags = collection.tags
        self.save(update_fields=["tags", "permission_group", "collection"])

    @move_to_container
    def move_to_entity(self, entity):
        """Move data object to entity."""
        if entity is None and self.in_container():
            raise ValidationError("Data object must not be removed from container.")
        self.entity = entity
        if entity:
            if entity.collection:
                self.move_to_collection(entity.collection)
            else:
                self.permission_group = entity.permission_group
                self.tags = entity.tags
        self.save(update_fields=["permission_group", "entity", "tags"])

    @classmethod
    def validate_change_containers(cls, data, entity, collection):
        """Validate changing entity and collection.

        Validate that:
        - data is not removed from the container.
        - if entity is given its collection must be the same as given collection
        """
        if data and data.in_container():
            if (data.entity and not entity) or (data.collection and not collection):
                raise ValidationError(
                    "Data object can not be removed from the container."
                )
        if entity and entity.collection != collection:
            raise ValidationError(
                "Entity must belong to the same collection as data object."
            )

    def move_to_containers(self, entity, collection):
        """Move the data object to the given entity and collection."""
        Data.validate_change_containers(self, entity, collection)
        if self.entity != entity or self.collection != collection:
            self.collection = collection
            self.entity = entity
            self.tags = getattr(collection, "tags", []) or getattr(entity, "tags", [])
            self.save(update_fields=["collection", "entity", "tags"])

    def validate_change_collection(self, collection):
        """Raise validation error if data object cannot change collection."""
        if self.entity and self.entity.collection != collection:
            raise ValidationError(
                "If Data is in entity, you can only move it to another collection "
                "by moving entire entity."
            )
        if collection is None:
            raise ValidationError("Data object can not be removed from the container.")

    def validate_change_entity(self, entity):
        """Raise validation error if data object cannot change entity."""
        if entity is None:
            raise ValidationError("Data object can not be removed from the container.")

        if self.collection and self.collection != entity.collection:
            raise ValidationError(
                "If data is in collection, you can only move it to another entity "
                "in the same collection."
            )

    def _render_name(self):
        """Render data name.

        The rendering is based on name template (`process.data_name`) and
        input context.

        """
        if not self.process.data_name or self.named_by_user:
            return

        inputs = copy.deepcopy(self.input)
        hydrate_input_references(
            inputs, self.process.input_schema, hydrate_values=False
        )
        template_context = inputs

        try:
            name = render_template(
                self.process, self.process.data_name, template_context
            )
        except EvaluationError:
            name = "?"

        self.name = name

    def get_resource_limits(self):
        """Return the resource limits for this data."""
        return self.process.get_resource_limits(self)

    def restart(self, resource_overrides: dict = {}):
        """Restart the data object and all its children.

        The status of the data object must be ERROR and stasus of its dependencies must
        be DONE.

        :param resource_overrides: dictionary mapping ids of data objects to resource
            overrides.

        :raises RuntimeError: if the object is not in the right state.
        :raises RuntimeError: when object dependencies are not in the status DONE.
        """

        def get_dependencies(data: Data) -> set[Data]:
            """Get the dependencies of the data object.

            Also include the data object itself.
            """
            dependencies = set()
            stack = [data]
            while stack:
                if (processing := stack.pop()) not in dependencies:
                    stack += Data.objects.filter(
                        parents_dependency__parent=processing,
                        parents_dependency__kind__in=[DataDependency.KIND_IO],
                    )
                    dependencies.add(processing)
            return dependencies

        def reset_data(data: Data):
            """Prepare the data object for another processing."""
            assert data.status == Data.STATUS_ERROR
            reset_dict: dict[str, Any] = {
                "started": None,
                "finished": None,
                "status": Data.STATUS_RESOLVING,
                "process_info": [],
                "process_warning": [],
                "process_error": [],
                "size": 0,
                "process_progress": 0,
                "process_rc": None,
                "process_pid": None,
                "descriptor": {},
                "descriptor_dirty": False,
                "duplicated": None,
            }
            for attribute, value in reset_dict.items():
                setattr(data, attribute, value)
            if hasattr(data, "worker"):
                # The worker can be saved here as it is not observable.
                data.worker.status = Worker.STATUS_PREPARING
                data.worker.save()

        # Avoid circular dependencies.
        from resolwe.flow.signals import commit_signal

        # Sanity check: only data in status ERROR can be restarted.
        if self.status != Data.STATUS_ERROR:
            raise RuntimeError(
                f"Only data in status {Data.STATUS_ERROR} can be restarted, not {self.status}."
            )

        # Sanity check: all dependencies must be in status DONE.
        if self.dependency_status() != Data.STATUS_DONE:
            raise RuntimeError(
                f"Data dependencies must have status '{Data.STATUS_DONE}'."
            )

        with transaction.atomic():
            # When data object is restarted we also need to restart all its children.
            # We have to clear them all and reset their statuses to be restarted.
            dependencies = get_dependencies(self)

            # Subprocess dependencies must be deleted, they will be recreated.
            Data.objects.filter(
                parents_dependency__parent=self,
                parents_dependency__kind__in=[DataDependency.KIND_SUBPROCESS],
            ).delete()

            # Construct the map id -> data.
            to_process = {data.id: data for data in dependencies}

            # Prevent circular import.
            from resolwe.flow.managers.listener.listener import cache_manager

            # Clear the Redis cache for objects to be restarted.
            for data_id in to_process.keys():
                cache_manager.clear(Data, (data_id,))

            # Evaluate lazy generator by listing it.
            list(map(reset_data, to_process.values()))

            # Set the overrides.
            for data_pk, override in resource_overrides.items():
                if int(data_pk) in to_process:
                    to_process[int(data_pk)].process_resources = override

            # Save the data objects. This will send out notifications.
            for data in to_process.values():
                data.save()

        # Start processing the duplicate.
        commit_signal(self, False, update_fields=None)

    def dependency_status(self) -> Optional[str]:
        """Return abstracted status of instance IO dependencies.

        :returns:
            - ``STATUS_ERROR`` .. one dependency has error status or was deleted
            - ``STATUS_DONE`` .. all dependencies have done status
            - ``None`` .. other
        """
        parents_statuses = set(
            DataDependency.objects.filter(child=self, kind=DataDependency.KIND_IO)
            .distinct("parent__status")
            .values_list("parent__status", flat=True)
        )

        if not parents_statuses:
            return Data.STATUS_DONE

        if None in parents_statuses:
            # Some parents have been deleted.
            return Data.STATUS_ERROR

        if Data.STATUS_ERROR in parents_statuses:
            return Data.STATUS_ERROR

        if len(parents_statuses) == 1 and Data.STATUS_DONE in parents_statuses:
            return Data.STATUS_DONE

        return None


class DataDependency(models.Model):
    """Dependency relation between data objects."""

    #: child uses parent's output as its input
    KIND_IO = "io"
    #: child was spawned by the parent
    KIND_SUBPROCESS = "subprocess"
    KIND_DUPLICATE = "duplicate"
    KIND_CHOICES = (
        (KIND_IO, "Input/output dependency"),
        (KIND_SUBPROCESS, "Subprocess"),
        (KIND_DUPLICATE, "Duplicate"),
    )

    #: child data object
    child = models.ForeignKey(
        Data, on_delete=models.CASCADE, related_name="parents_dependency"
    )
    #: parent data object
    parent = models.ForeignKey(
        Data, null=True, on_delete=models.SET_NULL, related_name="children_dependency"
    )
    #: kind of dependency
    kind = models.CharField(max_length=16, choices=KIND_CHOICES)
