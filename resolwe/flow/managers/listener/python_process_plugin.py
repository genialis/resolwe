"""Command handlers for python processes."""
import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields.jsonb import JSONField
from django.db.models import Model, QuerySet

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.models import Collection, Data, Process
from resolwe.flow.models.utils import serialize_collection_relations
from resolwe.flow.utils import dict_dot
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.utils import BraceMessage as __

from .plugin import ListenerPlugin

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor

logger = logging.getLogger(__name__)
UserClass = get_user_model()


class PermissionManager:
    """Permission manager."""

    def __init__(self):
        """Initialization."""
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

    def can_create(self, user, full_model_name, attributes) -> bool:
        """Query if user can create the given model.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_create(user, attributes)

    def can_update(self, user, full_model_name, model_instance, attributes) -> bool:
        """Query if user can update the given model.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_update(
            user, model_instance, attributes
        )

    def filter_objects(self, user, full_model_name, queryset) -> QuerySet:
        """Filter the objects for the given user.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].filter_objects(user, queryset)

    def can_read(self, user, full_model_name) -> bool:
        """Query if user can read the metadata of the given model.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_read(user)


permission_manager = PermissionManager()


class ExposeObjectPlugin(metaclass=abc.ABCMeta):
    """Plugin for exposing models in Python processes."""

    full_model_name = "app_label.model_name"

    def can_create(self, user: UserClass, attributes: dict) -> bool:
        """Can user create the model with given attributes."""
        return False

    def can_update(
        self, user: UserClass, model_instance: Model, attributes: Dict
    ) -> bool:
        """Can user update the given model instance."""
        return False

    def filter_objects(self, user: UserClass, queryset: QuerySet) -> QuerySet:
        """Filter the objects for the given user."""
        permission_name = f"view_{self.full_model_name.split('.')[1].lower()}"
        return get_objects_for_user(user, permission_name, queryset)

    def can_read(self, user: UserClass) -> bool:
        """Can read model structural info."""
        return True

    @classmethod
    def __init_subclass__(cls: Type["ExposeObjectPlugin"], **kwargs):
        """Register class with the permission manager on initialization."""
        super().__init_subclass__(**kwargs)
        permission_manager.add_plugin(cls)


class ExposeData(ExposeObjectPlugin):
    """Expose the Data model."""

    full_model_name = "flow.Data"

    def _has_permission(
        self, user: UserClass, model: Model, model_pk: int, permission_name: str
    ):
        """Check if contributor has requested permissions.

        :raises RuntimeError: with detailed explanation when check fails.
        """
        object_ = model.objects.filter(pk=model_pk)
        filtered_object = get_objects_for_user(
            user,
            [permission_name],
            object_,
        )
        if not filtered_object:
            if object_:
                raise RuntimeError(
                    f"No permissions: {model._meta.model_name} with id {model_pk}."
                )
            else:
                raise RuntimeError(
                    f"Object {model._meta.model_name} with id {model_pk} not found."
                )

    def can_create(self, user: UserClass, model_data: Dict) -> bool:
        """Can user update the given model instance."""
        allowed_fields = {
            "process_id",
            "output",
            "input",
            "tags",
            "entity_id",
            "collection_id",
            "name",
        }
        not_allowed_keys = set(model_data.keys()) - allowed_fields
        if not_allowed_keys:
            raise RuntimeError(f"Not allowed to set {','.join(not_allowed_keys)}.")

        # Check process permissions.
        self._has_permission(user, Process, model_data["process_id"], "view_process")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Process, model_data["entity_id"], "edit_entity")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(
                user, Collection, model_data["collection_id"], "edit_collection"
            )

    def can_update(
        self, user: UserClass, model_instance: Model, model_data: Dict
    ) -> bool:
        """Can user update the given model instance."""
        allowed_fields = {
            "output",
            "tags",
            "entity_id",
            "collection_id",
            "name",
            "descriptor",
        }
        not_allowed_keys = set(model_data.keys()) - allowed_fields
        if not_allowed_keys:
            raise RuntimeError(f"Not allowed to set {','.join(not_allowed_keys)}.")

        # Check permission to modify the Data object.
        self._has_permission(user, Data, model_instance.id, "edit_data")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Process, model_data["entity_id"], "edit_entity")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(
                user, Collection, model_data["collection_id"], "edit_collection"
            )


class ExposeEntity(ExposeObjectPlugin):
    """Expose the Entity model."""

    full_model_name = "flow.Entity"


class ExposeCollection(ExposeObjectPlugin):
    """Expose the Collection model."""

    full_model_name = "flow.Collection"


class ExposeProcess(ExposeObjectPlugin):
    """Expose the Process model."""

    full_model_name = "flow.Process"


class ExposeStorage(ExposeObjectPlugin):
    """Expose the Storage model."""

    # TODO: permission based on permission on Data object.
    full_model_name = "flow.Storage"


class PythonProcess(ListenerPlugin):
    """Handler methods for Python processes."""

    name = "PythonProcess plugin"

    ALLOWED_MODELS_C = ["Collection", "Entity", "Data"]
    ALLOWED_MODELS_RW = ["Data", "Process", "Entity", "Collection", "Storage"]
    WRITE_FIELDS = {"Data.name", "Data.output", "Data.descriptor"}

    def __init__(self):
        """Initialize plugin."""
        self._permission_manager = permission_manager
        super().__init__()

    def _has_permission(self, model, model_pk: int, permission_name: str, contributor):
        """Check if contributor has requested permissions.

        :raises RuntimeError: with detailed explanation when check fails.
        """
        object_ = model.objects.filter(pk=model_pk)
        filtered_object = get_objects_for_user(
            contributor,
            [permission_name],
            object_,
        )
        if not filtered_object:
            if object_:
                raise RuntimeError(
                    f"No permissions: {model._meta.model_name} with id {model_pk}."
                )
            else:
                raise RuntimeError(
                    f"Object {model._meta.model_name} with id {model_pk} not found."
                )

    def _can_modify_data(self, contributor, **model_data: Dict[str, Any]):
        """Check if user has the permission to modify a Data object.

        User can always create a data object but also need edit permission
        to the collection for example.

        :raises RuntimeError: witd detailed description when user can not
            create Data object.
        """
        can_set = {
            "process_id",
            "output",
            "input",
            "tags",
            "entity_id",
            "collection_id",
        }
        not_allowed_keys = set(model_data.keys()) - can_set
        if not_allowed_keys:
            message = f"Not allowed to set {','.join(not_allowed_keys)} not allowed."
            raise RuntimeError(message)

        # Check process permissions.
        self._has_permission(
            Process, model_data["process_id"], "view_process", contributor
        )

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(
                Process, model_data["entity_id"], "edit_entity", contributor
            )

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(
                Collection,
                model_data["collection_id"],
                "edit_collection",
                contributor,
            )

    def handle_create_object(
        self, message: Message[Tuple[str, Dict[str, Any]]], manager: "Processor"
    ) -> Response[int]:
        """Create new object and return its id.

        :raises RuntimeError: if user has no permission to create the object.
        """
        # TODO: modify Python processes to also send app name with the table!
        app_name, model_name, model_data = message.message_data
        full_model_name = f"{app_name}.{model_name}"
        self._permission_manager.can_create(
            manager.contributor, full_model_name, model_data
        )
        model = apps.get_model(app_name, model_name)
        model_data["contributor"] = manager.contributor
        return message.respond_ok(model.objects.create(**model_data).id)

    def handle_filter_objects(
        self, message: Message[Tuple[str, Dict[str, Any]]], manager: "Processor"
    ) -> Response[List[int]]:
        """Get a list of objects based on criteria.

        List is further filtered based on user permissions.
        """
        app_name, model_name, filters, attributes = message.message_data
        full_model_name = f"{app_name}.{model_name}"
        model = apps.get_model(app_name, model_name)
        filtered_objects = self._permission_manager.filter_objects(
            manager.contributor, full_model_name, model.objects.filter(**filters)
        )
        return message.respond_ok(list(filtered_objects.values_list(*attributes)))

    def handle_update_model_fields(
        self, message: Message[Tuple[str, int, Dict[str, Any]]], manager: "Processor"
    ) -> Response[str]:
        """Update the value for the given fields.

        The received message format is
        (app_name, model name, model primary key, names -> values).

        Field name can be given in dot notation for JSON fields.

        :raises RuntimeError: if user has no permissions to modify the object.
        """
        app_name, model_name, model_pk, mapping = message.message_data
        full_model_name = f"{app_name}.{model_name}"

        # The most common request is for the data object we are processing.
        # Avoid hitting the database in such case.
        if full_model_name == "flow.Data" and model_pk == manager.data_id:
            model_instance = manager.data
            model = Data
        else:
            model = apps.get_model(app_name, model_name)
            model_instance = model.objects.filter(pk=model_pk).get()

        self._permission_manager.can_update(
            manager.contributor, full_model_name, model_instance, mapping
        )

        for field_name, field_value in mapping.items():
            if isinstance(model._meta.get_field(field_name), JSONField):
                current_value = getattr(model_instance, field_name)
                for key, value in field_value.items():
                    dict_dot(current_value, key, value)
            else:
                setattr(model_instance, field_name, field_value)
        model_instance.save(update_fields=mapping.keys())
        return message.respond_ok("OK")

    def handle_get_model_fields_details(
        self, message: Message[str], manager: "Processor"
    ) -> Response[Dict[str, Tuple[str, Optional[str]]]]:
        """Get the field names and types for the given model.

        The response is a dictionary which field maps names to its types.

        :raises RuntimeError: if user has no permission to read the metadata.
        """
        app_name, model_name = message.message_data
        full_model_name = f"{app_name}.{model_name}"

        self._permission_manager.can_read(manager.contributor, full_model_name)
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
        self, message: Message[Tuple[str, int, List[str]]], manager: "Processor"
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
            manager.contributor, full_model_name, model.objects.filter(pk=model_pk)
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
        self, message: Message[int], manager: "Processor"
    ) -> Response[List[dict]]:
        """Get relations for the given collection object."""
        collection = get_objects_for_user(
            manager.contributor,
            "view_collection",
            Collection.objects.filter(id=message.message_data),
        ).get()
        return message.respond_ok(serialize_collection_relations(collection))

    def handle_get_process_requirements(
        self, message: Message[int], manager: "Processor"
    ) -> Response[dict]:
        """Return the requirements for the process with the given id."""
        process_id = message.message_data
        filtered_process = get_objects_for_user(
            manager.contributor, ["process_view"], Process.objects.filter(pk=process_id)
        )
        process_requirements, process_slug = filtered_process.values_list(
            "requirements", "slug"
        ).get()
        resources = process_requirements.get("resources", {})

        # Get limit defaults and overrides.
        limit_defaults = getattr(settings, "FLOW_PROCESS_RESOURCE_DEFAULTS", {})
        limit_overrides = getattr(settings, "FLOW_PROCESS_RESOURCE_OVERRIDES", {})

        limits = {}
        limits["cores"] = int(resources.get("cores", 1))
        max_cores = getattr(settings, "FLOW_PROCESS_MAX_CORES", None)
        if max_cores:
            limits["cores"] = min(limits["cores"], max_cores)

        memory = limit_overrides.get("memory", {}).get(process_slug, None)
        if memory is None:
            memory = int(
                resources.get(
                    "memory",
                    # If no memory resource is configured, check settings.
                    limit_defaults.get("memory", 4096),
                )
            )
        limits["memory"] = memory
        process_requirements["resources"] = limits
        return message.respond_ok(process_requirements)
