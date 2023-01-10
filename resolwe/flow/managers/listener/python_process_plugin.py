"""Command handlers for python processes."""
import abc
import importlib
import logging
import os
from base64 import b64encode
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type, Union
from zipfile import ZIP_STORED, ZipFile

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields.jsonb import JSONField as JSONFieldb
from django.db.models import ForeignKey, JSONField, ManyToManyField, Model, QuerySet

from resolwe.flow.executors import constants
from resolwe.flow.executors.socket_utils import Message, Response, retry
from resolwe.flow.models import (
    Collection,
    Data,
    DescriptorSchema,
    Entity,
    Process,
    Storage,
)
from resolwe.flow.models.base import UniqueSlugError
from resolwe.flow.models.utils import serialize_collection_relations
from resolwe.flow.utils import dict_dot
from resolwe.permissions.models import Permission
from resolwe.storage.connectors import connectors
from resolwe.storage.models import FileStorage
from resolwe.test.utils import is_testing
from resolwe.utils import BraceMessage as __

from .plugin import ListenerPlugin

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor

logger = logging.getLogger(__name__)
UserClass = get_user_model()

# How many times to retry creating new object on slug collision error.
OBJECT_CREATE_RETRIES = 10


class PermissionManager:
    """Permission manager class."""

    def __init__(self):
        """Initialize."""
        self._plugins: Dict[str, "ExposeObjectPlugin"] = dict()

    def add_plugin(self, plugin_class: Type["ExposeObjectPlugin"]):
        """Add new permission plugin.

        :raises AssertionError: when plugin handling the same object is already
            registered.
        """
        plugin_name = plugin_class.full_model_name
        assert (
            plugin_name not in self._plugins
        ), "Plugin for model {plugin_name} already registered."
        self._plugins[plugin_name] = plugin_class()
        logger.debug(__("Registering permission plugin {}.", plugin_name))

    def can_handle(self, full_model_name: str) -> bool:
        """Query permission manager if it can handle given model."""
        return full_model_name in self._plugins

    def can_create(self, user, full_model_name: str, attributes, data_id: int):
        """Query if user can create the given model.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_create(user, attributes, data_id)

    def can_update(
        self,
        user,
        full_model_name: str,
        model_instance: Model,
        attributes,
        data_id: int,
    ):
        """Query if user can update the given model.

        :raises RuntimeError: if user does not have permissions to update the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_update(
            user, model_instance, attributes, data_id
        )

    def filter_objects(
        self, user, full_model_name: str, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].filter_objects(user, queryset, data_id)

    def can_read(self, user, full_model_name: str, data_id: int):
        """Query if user can read the metadata of the given model.

        :raises RuntimeError: if user does not have permissions to read the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_read(user, data_id)


permission_manager = PermissionManager()


class ExposeObjectPlugin(metaclass=abc.ABCMeta):
    """Plugin for exposing models in Python processes."""

    full_model_name = "app_label.model_name"

    def _has_permission(
        self, user: UserClass, model: Type[Model], model_pk: int, permission_name: str
    ):
        """Check if contributor has requested permissions.

        :raises RuntimeError: with detailed explanation when check fails.
        """
        object_ = model.objects.filter(pk=model_pk)
        if not object_.exists():
            raise RuntimeError(
                f"Object {model._meta.model_name} with id {model_pk} not found."
            )

        if not object_.filter_for_user(
            user, Permission.from_name(permission_name)
        ).exists():
            if object_:
                raise RuntimeError(
                    f"No edit permission for {model._meta.model_name} with id {model_pk}."
                )
            else:
                raise RuntimeError(
                    f"Object {model._meta.model_name} with id {model_pk} not found."
                )

    def can_create(self, user: UserClass, attributes: dict, data_id: int):
        """Can user create the model with given attributes.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """
        raise RuntimeError(
            (
                f"User {user} has no permission to create model"
                f"{self.full_model_name} with attributes {attributes}."
            )
        )

    def can_update(
        self, user: UserClass, model_instance: Model, attributes: Dict, data_id: int
    ):
        """Can user update the given model instance.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
        raise RuntimeError(
            (
                f"User {user} has no permission to update model"
                f"{self.full_model_name} with attributes {attributes}."
            )
        )

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return queryset.filter_for_user(user)

    def can_read(self, user: UserClass, data_id: int):
        """Can read model structural info.

        :raises RuntimeError: when user does not have permissions to access
            the given model.
        """

    @classmethod
    def __init_subclass__(cls: Type["ExposeObjectPlugin"], **kwargs):
        """Register class with the permission manager on initialization."""
        super().__init_subclass__(**kwargs)
        permission_manager.add_plugin(cls)


class ExposeData(ExposeObjectPlugin):
    """Expose the Data model."""

    full_model_name = "flow.Data"

    def can_create(self, user: UserClass, model_data: Dict, data_id: int):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """
        allowed_fields = {
            "process_id",
            "output",
            "input",
            "tags",
            "entity",
            "entity_id",
            "collection",
            "collection_id",
            "name",
        }
        not_allowed_keys = set(model_data.keys()) - allowed_fields
        if not_allowed_keys:
            raise RuntimeError(f"Not allowed to set {','.join(not_allowed_keys)}.")

        # Check process permissions.
        self._has_permission(user, Process, model_data["process_id"], "view")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Entity, model_data["entity_id"], "edit")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(user, Collection, model_data["collection_id"], "edit")

    def can_update(
        self, user: UserClass, model_instance: Data, model_data: Dict, data_id: int
    ):
        """Can user update the given model instance.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
        allowed_fields = {
            "output",
            "tags",
            "entity_id",
            "entity",
            "collection_id",
            "collection",
            "name",
            "descriptor_schema_id",
            "descriptor_schema",
            "descriptor",
        }
        not_allowed_keys = set(model_data.keys()) - allowed_fields
        if not_allowed_keys:
            raise RuntimeError(f"Not allowed to set {','.join(not_allowed_keys)}.")

        # Check permission to modify the Data object. The current data object
        # can always be modified.
        if model_instance.id != data_id:
            self._has_permission(user, Data, model_instance.id, "edit")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Entity, model_data["entity_id"], "edit")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(user, Collection, model_data["collection_id"], "edit")
        if "descriptor_schema_id" in model_data:
            # Check DescriptorSchema permissions.
            self._has_permission(
                user, DescriptorSchema, model_data["descriptor_schema_id"], "view"
            )

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user."""
        parent_ids = Data.objects.filter(pk=data_id).values_list("parents")
        inputs = queryset.filter(id__in=parent_ids)
        return queryset.filter_for_user(user).distinct().union(inputs)


class ExposeDescriptorSchema(ExposeObjectPlugin):
    """Expose the DescriptorSchema model."""

    full_model_name = "flow.DescriptorSchema"


class ExposeUser(ExposeObjectPlugin):
    """Expose the User model.

    Used to read contributor data.
    """

    full_model_name = UserClass._meta.label

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return queryset.filter(pk=user.id)


class ExposeEntity(ExposeObjectPlugin):
    """Expose the Entity model."""

    full_model_name = "flow.Entity"

    def can_create(self, user: UserClass, attributes: dict, data_id: int):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """

    def can_update(
        self, user: UserClass, model_instance: Data, model_data: Dict, data_id: int
    ):
        """Can user update the given model instance.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
        allowed_fields = {
            "description",
            "tags",
            "name",
            "descriptor",
        }
        not_allowed_keys = set(model_data.keys()) - allowed_fields
        if not_allowed_keys:
            raise RuntimeError(f"Not allowed to set {','.join(not_allowed_keys)}.")

        # Check permission to modify the Entity object.
        self._has_permission(user, Entity, model_instance.id, "edit")


class ExposeCollection(ExposeObjectPlugin):
    """Expose the Collection model."""

    full_model_name = "flow.Collection"

    def can_create(self, user: UserClass, attributes: dict, data_id: int):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """


class ExposeProcess(ExposeObjectPlugin):
    """Expose the Process model."""

    full_model_name = "flow.Process"

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user."""
        parent_ids = Data.objects.filter(pk=data_id).values_list("parents")
        processes_of_inputs = queryset.filter(data__in=parent_ids).distinct()
        return queryset.filter_for_user(user).distinct().union(processes_of_inputs)


class ExposeStorage(ExposeObjectPlugin):
    """Expose the Storage model."""

    # TODO: permission based on permission on Data object.
    full_model_name = "flow.Storage"

    def can_create(self, user: UserClass, attributes: dict, data_id: int):
        """Can user create the model with given attributes.

        :raises RuntimeError: when user does not have permissions to create
            the given model.
        """

    def can_update(
        self, user: UserClass, model_instance: Storage, model_data: Dict, data_id: int
    ):
        """Can user update the given Storage object.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
        processed_data_ids = set()
        # User must have permission to modify all the data objects this object belongs to.
        for data in model_instance.data.all():
            permission_manager.can_update(
                user, "flow.Data", data, {"output": {}}, data_id
            )
            processed_data_ids.add(data_id)

        # Only contributor can modify Storage object if it is orphaned.
        if not processed_data_ids and model_instance.contributor != user:
            raise RuntimeError(
                "Only contributor can modify unassigned Storage object with "
                f"id {model_instance.pk}, modification attempted as {user}."
            )

        # When adding storage to Data objects check permissions to modify the
        # data objects. Since permission checks are slow only process the data
        # objects that were not processed above.
        for data_id in model_data.get("data", []):
            if data_id not in processed_data_ids:
                permission_manager.can_update(
                    user,
                    "flow.Data",
                    Data.objects.get(pk=data_id),
                    {"output": {}},
                    data_id,
                )

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data_id: int
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return (
            queryset.filter_for_user(user)
            .distinct()
            .union(queryset.filter(contributor=user))
        )


class PythonProcess(ListenerPlugin):
    """Handler methods for Python processes."""

    name = "PythonProcess plugin"

    ALLOWED_MODELS_C = ["Collection", "Entity", "Data"]
    ALLOWED_MODELS_RW = ["Data", "Process", "Entity", "Collection", "Storage"]
    WRITE_FIELDS = {"Data.name", "Data.output", "Data.descriptor"}

    def __init__(self):
        """Initialize plugin."""
        self._permission_manager = permission_manager
        self._hydrate_cache: Dict[int, str] = dict()
        super().__init__()

    def handle_resolve_data_path(
        self, data_id: int, message: Message[int], manager: "Processor"
    ) -> Response[str]:
        """Return the base path that stores given data."""
        data_pk = message.message_data
        if data_pk not in self._hydrate_cache or is_testing():
            mount_point = os.fspath(constants.INPUTS_VOLUME)
            data_connectors = FileStorage.objects.get(data__pk=data_pk).connectors
            for connector in connectors.for_storage("data"):
                if connector in data_connectors and connector.mountable:
                    mount_point = f"/data_{connector.name}"
                    break
            self._hydrate_cache[data_pk] = mount_point
        return message.respond_ok(self._hydrate_cache[data_pk])

    def handle_get_python_program(
        self,
        data_id: int,
        message: Message[Tuple[str, Dict[str, Any]]],
        manager: "Processor",
    ) -> Response[str]:
        """Return the process source code for the given data object."""
        run_dict = (
            Process.objects.filter(data__id=data_id)
            .values_list("run", flat=True)
            .last()
        ) or {}
        return message.respond_ok(run_dict.get("program", ""))

    def handle_create_object(
        self,
        data_id: int,
        message: Message[Tuple[str, str, Dict[str, Any]]],
        manager: "Processor",
    ) -> Response[int]:
        """Create new object and return its id.

        :raises RuntimeError: if user has no permission to create the object.
        """

        @retry(
            max_retries=OBJECT_CREATE_RETRIES,
            retry_exceptions=(UniqueSlugError,),
            min_sleep=1,
            max_sleep=1,
        )
        def create_model(model: Type[Model], model_data: Dict[str, Any]):
            """Create the model.

            Retry up to 10 times on slug colision error.
            """
            return model.objects.create(**model_data)

        app_name, model_name, model_data = message.message_data
        full_model_name = f"{app_name}.{model_name}"
        self._permission_manager.can_create(
            manager.contributor(data_id), full_model_name, model_data, data_id
        )
        model = apps.get_model(app_name, model_name)
        model_data["contributor_id"] = manager.contributor(data_id).id
        return message.respond_ok(create_model(model, model_data).id)

    def handle_filter_objects(
        self,
        data_id: int,
        message: Message[
            Union[
                Tuple[str, str, Dict[str, Any], List[str], List[str]],
                Tuple[str, str, Dict[str, Any], List[str]],
            ],
        ],
        manager: "Processor",
    ) -> Response[List[int]]:
        """Get a list of objects based on criteria.

        List is further filtered based on user permissions and sorted according
        to the list of strings the user provides.
        """
        # Sorting was added later. For compatibility reasons handle both
        # message types, remove the one without sorting ASAP.
        sorting: List[str] = []
        if len(message.message_data) == 4:
            app_name, model_name, filters, attributes = message.message_data
        elif len(message.message_data) == 5:
            app_name, model_name, filters, sorting, attributes = message.message_data
        else:
            raise RuntimeError(
                "Message in handle_filter_objects for the object "
                f"{data_id} not in the correct format. Got tuple of"
                f"length {len(message.message_data)}, expected length 4 or 5."
            )

        full_model_name = f"{app_name}.{model_name}"
        model = apps.get_model(app_name, model_name)
        filtered_objects = self._permission_manager.filter_objects(
            manager.contributor(data_id),
            full_model_name,
            model.objects.filter(**filters),
            data_id,
        )
        return message.respond_ok(
            list(filtered_objects.order_by(*sorting).values_list(*attributes))
        )

    def handle_update_model_fields(
        self,
        data_id: int,
        message: Message[Tuple[str, str, int, Dict[str, Any]]],
        manager: "Processor",
    ) -> Response[str]:
        """Update the value for the given fields.

        The received message format is
        (app_name, model name, model primary key, names -> values).

        Field name can be given in dot notation for JSON fields.

        :raises RuntimeError: if user has no permissions to modify the object.
        """
        app_name, model_name, model_pk, mapping = message.message_data
        full_model_name = f"{app_name}.{model_name}"

        model = apps.get_model(app_name, model_name)
        model_instance = model.objects.get(pk=model_pk)

        self._permission_manager.can_update(
            manager.contributor(data_id),
            full_model_name,
            model_instance,
            mapping,
            data_id,
        )

        # Update all fields except m2m.
        update_fields = []
        for field_name, field_value in mapping.items():
            # Not exactly sure how to handle this. Output is a JSONField and is
            # only updated, other JSON fields should probably be replaced.
            # Compromise: when update is a dict, then only values in dict are
            # updates, else replaced.
            field_meta = model._meta.get_field(field_name)
            if isinstance(field_meta, (JSONField, JSONFieldb)) and isinstance(
                field_value, dict
            ):
                update_fields.append(field_name)
                current_value = getattr(model_instance, field_name)
                for key, value in field_value.items():
                    dict_dot(current_value, key, value)
            elif isinstance(field_meta, ManyToManyField):
                assert isinstance(
                    field_value, list
                ), "Only lists may be assigned to many-to-many relations"
                field = getattr(model_instance, field_name)
                field_value_set = set(field_value)
                current_objects = set(field.all().values_list("pk", flat=True))
                objects_to_add = field_value_set - current_objects
                objects_to_remove = current_objects - field_value_set
                if objects_to_remove:
                    field.remove(*objects_to_remove)
                if objects_to_add:
                    field.add(*objects_to_add)
            # Set ID directly when setting foreign key relations.
            elif isinstance(field_meta, ForeignKey):
                field_name = f"{field_name}_id"
                update_fields.append(field_name)
                setattr(model_instance, field_name, field_value)
            else:
                update_fields.append(field_name)
                setattr(model_instance, field_name, field_value)
        model_instance.save(update_fields=update_fields)
        return message.respond_ok("OK")

    def handle_get_model_fields_details(
        self, data_id: int, message: Message[Tuple[str, str]], manager: "Processor"
    ) -> Response[Dict[str, Tuple[str, bool, Any]]]:
        """Get the field names and types for the given model.

        The response is a dictionary which field maps names to its types.

        :raises RuntimeError: if user has no permission to read the metadata.
        """
        app_name, model_name = message.message_data
        full_model_name = f"{app_name}.{model_name}"

        self._permission_manager.can_read(
            manager.contributor(data_id), full_model_name, data_id
        )
        model = apps.get_model(app_name, model_name)
        response = {}
        for field in model._meta.get_fields():
            related_model = None
            if field.is_relation:
                related_model = field.related_model._meta.label
            response[field.name] = (
                field.get_internal_type(),
                not field.null,
                related_model,
            )
        return message.respond_ok(response)

    def handle_get_model_fields(
        self,
        data_id: int,
        message: Message[Tuple[str, str, int, List[str]]],
        manager: "Processor",
    ) -> Response[Dict[str, Any]]:
        """Return the value of the given model for the given fields.

        The received message format is
        (model_name, primary_key, list_of_fields).

        In case of JSON field the field name can contain underscores to get
        only the part of the JSON we are interested in.
        """
        app_name, model_name, model_pk, field_names = message.message_data
        full_model_name = f"{app_name}.{model_name}"

        model = apps.get_model(app_name, model_name)
        filtered_objects = self._permission_manager.filter_objects(
            manager.contributor(data_id),
            full_model_name,
            model.objects.filter(pk=model_pk),
            data_id,
        )
        values = filtered_objects.values(*field_names)
        # NOTE: non JSON serializable fields are NOT supported. If such field
        # is requested the exception will be handled one level above and
        # response with status error will be returned.
        if values.count() == 1:
            values = values.get()
        else:
            values = dict()
        return message.respond_ok(values)

    def handle_get_relations(
        self, data_id: int, message: Message[int], manager: "Processor"
    ) -> Response[List[dict]]:
        """Get relations for the given collection object."""
        contributor = manager.contributor(data_id)
        collection = (
            Collection.objects.filter(id=message.message_data)
            .filter_for_user(contributor)
            .get()
        )
        return message.respond_ok(serialize_collection_relations(collection))

    def handle_get_process_requirements(
        self, data_id: int, message: Message[int], manager: "Processor"
    ) -> Response[dict]:
        """Return the requirements for the process with the given id."""
        process_id = message.message_data
        filtered_process = Process.objects.filter(pk=process_id).filter_for_user(
            manager.contributor(data_id)
        )[0]

        process_limits = filtered_process.get_resource_limits()
        process_requirements = filtered_process.requirements
        process_requirements["resources"] = process_limits
        return message.respond_ok(process_requirements)

    def handle_get_self_requirements(
        self, data_id: int, message: Message[int], manager: "Processor"
    ) -> Response[dict]:
        """Return the requirements for the process being executed."""
        data = manager.data(data_id)
        limits = data.get_resource_limits()
        process_requirements = data.process.requirements
        process_requirements["resources"] = limits
        return message.respond_ok(process_requirements)

    def handle_get_python_runtime(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Return the Python Process runtime.

        It is gzipped and returned as b64encoded string.
        """

        zipped = BytesIO()
        with ZipFile(file=zipped, mode="w", compression=ZIP_STORED) as zip_handle:
            for runtime_class_name in settings.FLOW_PROCESSES_RUNTIMES:
                module_name, class_name = runtime_class_name.rsplit(".", 1)
                source_path = importlib.util.find_spec(module_name).origin
                assert (
                    source_path is not None
                ), f"Unable to determine the source path of the module {module_name}."
                source_dir = Path(source_path).parent

                base_destination = Path(*module_name.split(".")[:-1])
                for source_entry in source_dir.rglob("*.py"):
                    relative_path = source_entry.relative_to(source_dir)
                    destination = base_destination / relative_path
                    zip_handle.write(source_entry, destination)

                # Create missing __init__.py files with empty content.
                for path in list(Path(*module_name.split(".")).parents)[1:-1]:
                    zip_destination = path / "__init__.py"
                    zip_handle.writestr(os.fspath(zip_destination), "")

        zipped.seek(0)
        return message.respond_ok(b64encode(zipped.read()).decode())

    def handle_get_user_model_label(
        self, data_id: int, message: Message[str], manager: "Processor"
    ) -> Response[str]:
        """Return the label of the custom user model.

        It is used to connect the model in Python process runtime with the
        custom user model.
        """
        return message.respond_ok(UserClass._meta.label)
