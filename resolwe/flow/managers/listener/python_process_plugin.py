"""Command handlers for python processes."""
import abc
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields.jsonb import JSONField
from django.db.models import ManyToManyField, Model, QuerySet

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.models import Collection, Data, Process, Storage
from resolwe.flow.models.utils import serialize_collection_relations
from resolwe.flow.utils import dict_dot
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import get_full_perm
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

    def can_create(self, user, full_model_name, attributes, data):
        """Query if user can create the given model.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_create(user, attributes, data)

    def can_update(self, user, full_model_name, model_instance, attributes, data):
        """Query if user can update the given model.

        :raises RuntimeError: if user does not have permissions to update the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_update(
            user, model_instance, attributes, data
        )

    def filter_objects(self, user, full_model_name, queryset, data) -> QuerySet:
        """Filter the objects for the given user.

        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].filter_objects(user, queryset, data)

    def can_read(self, user, full_model_name, data):
        """Query if user can read the metadata of the given model.

        :raises RuntimeError: if user does not have permissions to read the
            given model.
        :raises KeyError: if permission manager has no plugin registered for
            the given model.
        """
        return self._plugins[full_model_name].can_read(user, data)


permission_manager = PermissionManager()


class ExposeObjectPlugin(metaclass=abc.ABCMeta):
    """Plugin for exposing models in Python processes."""

    full_model_name = "app_label.model_name"

    def can_create(self, user: UserClass, attributes: dict, data: Data):
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
        self, user: UserClass, model_instance: Model, attributes: Dict, data: Data
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
        self, user: UserClass, queryset: QuerySet, data: Data
    ) -> QuerySet:
        """Filter the objects for the given user."""
        permission_name = get_full_perm("view", queryset.model)
        return get_objects_for_user(user, permission_name, queryset)

    def can_read(self, user: UserClass, data: Data):
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

    def _has_permission(
        self, user: UserClass, model: Type[Model], model_pk: int, permission_name: str
    ):
        """Check if contributor has requested permissions.

        :raises RuntimeError: with detailed explanation when check fails.
        """
        full_permission_name = get_full_perm(permission_name, model)
        object_ = model.objects.filter(pk=model_pk)
        filtered_object = get_objects_for_user(
            user,
            [full_permission_name],
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

    def can_create(self, user: UserClass, model_data: Dict, data: Data):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """
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
        self._has_permission(user, Process, model_data["process_id"], "view")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Process, model_data["entity_id"], "edit")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(user, Collection, model_data["collection_id"], "edit")

    def can_update(
        self, user: UserClass, model_instance: Data, model_data: Dict, data: Data
    ):
        """Can user update the given model instance.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
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
        self._has_permission(user, Data, model_instance.id, "edit")

        if "entity_id" in model_data:
            # Check entity permissions.
            self._has_permission(user, Process, model_data["entity_id"], "edit")

        if "collection_id" in model_data:
            # Check collection permissions.
            self._has_permission(user, Collection, model_data["collection_id"], "edit")

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data: Data
    ) -> QuerySet:
        """Filter the objects for the given user."""
        inputs = queryset.filter(id__in=data.parents.all())
        permission_name = get_full_perm("view", queryset.model)
        return (
            get_objects_for_user(user, permission_name, queryset)
            .distinct()
            .union(inputs)
        )


class ExposeUser(ExposeObjectPlugin):
    """Expose the User model.

    Used to read contributor data.
    """

    full_model_name = "auth.User"

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data: Data
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return queryset.filter(pk=user.pk)


class ExposeEntity(ExposeObjectPlugin):
    """Expose the Entity model."""

    full_model_name = "flow.Entity"


class ExposeCollection(ExposeObjectPlugin):
    """Expose the Collection model."""

    full_model_name = "flow.Collection"

    def can_create(self, user: UserClass, attributes: dict, data: Data):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """


class ExposeProcess(ExposeObjectPlugin):
    """Expose the Process model."""

    full_model_name = "flow.Process"

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data: Data
    ) -> QuerySet:
        """Filter the objects for the given user."""
        processes_of_inputs = queryset.filter(data__in=data.parents.all())
        permission_name = get_full_perm("view", queryset.model)
        return (
            get_objects_for_user(user, permission_name, queryset)
            .distinct()
            .union(processes_of_inputs)
        )


class ExposeStorage(ExposeObjectPlugin):
    """Expose the Storage model."""

    # TODO: permission based on permission on Data object.
    full_model_name = "flow.Storage"

    def can_create(self, user: UserClass, attributes: dict, data: Data):
        """Can user create the model with given attributes.

        :raises RuntimeError: when user does not have permissions to create
            the given model.
        """

    def can_update(
        self, user: UserClass, model_instance: Storage, model_data: Dict, data: Data
    ):
        """Can user update the given Storage object.

        :raises RuntimeError: when user does not have permissions to update
            the given model.
        """
        processed_data_ids = set()
        # User must have permission to modify all the data objects this object belongs to.
        for data in model_instance.data.all():
            permission_manager.can_update(user, "flow.Data", data, {"output": {}}, data)
            processed_data_ids.add(data.pk)

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
                    data,
                )

    def filter_objects(
        self, user: UserClass, queryset: QuerySet, data: Data
    ) -> QuerySet:
        """Filter the objects for the given user.

        Snorage objects are special: we have to check if user has permission on
        the data model. The permission name must be given in the form
        'app_label.permission_name' otherwise wrong content type is infered
        it the get_objects_for_user method.
        """
        permission_name = "flow." + get_full_perm("view", Data)
        perms_filter = "data__pk__in"
        return (
            get_objects_for_user(
                user, permission_name, queryset, perms_filter=perms_filter
            )
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
        super().__init__()

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
            manager.contributor, full_model_name, model_data, manager.data
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
            manager.contributor,
            full_model_name,
            model.objects.filter(**filters),
            manager.data,
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
            manager.contributor, full_model_name, model_instance, mapping, manager.data
        )

        # Update all fields except m2m.
        update_fields = []
        for field_name, field_value in mapping.items():
            # Not exactly sure how to handle this. Output is a JSONField and is
            # only updated, other JSON fields should probably be replaced.
            # Compromise: when update is a dict, then only values in dict are
            # updates, else replaced.
            if isinstance(model._meta.get_field(field_name), JSONField) and isinstance(
                field_value, dict
            ):
                update_fields.append(field_name)
                current_value = getattr(model_instance, field_name)
                for key, value in field_value.items():
                    dict_dot(current_value, key, value)
            elif isinstance(model._meta.get_field(field_name), ManyToManyField):
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
            else:
                update_fields.append(field_name)
                setattr(model_instance, field_name, field_value)
        model_instance.save(update_fields=update_fields)
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

        self._permission_manager.can_read(
            manager.contributor, full_model_name, manager.data
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
            manager.contributor,
            full_model_name,
            model.objects.filter(pk=model_pk),
            manager.data,
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
            get_full_perm("view", Collection),
            Collection.objects.filter(id=message.message_data),
        ).get()
        return message.respond_ok(serialize_collection_relations(collection))

    def handle_get_process_requirements(
        self, message: Message[int], manager: "Processor"
    ) -> Response[dict]:
        """Return the requirements for the process with the given id."""
        process_id = message.message_data
        filtered_process = get_objects_for_user(
            manager.contributor,
            get_full_perm("view", Process),
            Process.objects.filter(pk=process_id),
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
