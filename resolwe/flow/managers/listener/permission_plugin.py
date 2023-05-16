"""Define the class to handle exposing models to the python processes."""

from typing import TYPE_CHECKING, Type

from django.contrib.auth import get_user_model
from django.db.models import Model, QuerySet

from resolwe.flow.managers.listener.plugin_interface import Plugin, PluginManager
from resolwe.flow.models import (
    Collection,
    Data,
    DescriptorSchema,
    Entity,
    Process,
    Storage,
)
from resolwe.permissions.models import Permission

if TYPE_CHECKING:
    from django.contrib.auth.models import User as UserType

User = get_user_model()


class PermissionManager(PluginManager["ExposeObjectPlugin"]):
    """Permission manager class."""

    plugin_unique_identifier = "full_model_name"

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


class ExposeObjectPlugin(Plugin):
    """Plugin for exposing models in Python processes."""

    full_model_name = "app_label.model_name"
    plugin_manager = permission_manager
    abstract = True

    def _has_permission(
        self, user: "UserType", model: Type[Model], model_pk: int, permission_name: str
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

    def can_create(self, user: "UserType", attributes: dict, data_id: int):
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
        self, user: "UserType", model_instance: Model, attributes: dict, data_id: int
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
        self,
        user: "UserType",
        queryset: QuerySet,
        data_id: int,
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return queryset.filter_for_user(user)

    def can_read(self, user: "UserType", data_id: int):
        """Can read model structural info.

        :raises RuntimeError: when user does not have permissions to access
            the given model.
        """

    @classmethod
    def get_identifier(cls):
        """Get the plugin identifier."""
        return cls.full_model_name


class ExposeData(ExposeObjectPlugin):
    """Expose the Data model."""

    full_model_name = "flow.Data"

    def can_create(self, user: "UserType", model_data: dict, data_id: int):
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
        self, user: "UserType", model_instance: Data, model_data: dict, data_id: int
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
        self,
        user: "UserType",
        queryset: QuerySet,
        data_id: int,
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

    full_model_name = User._meta.label

    def filter_objects(
        self,
        user: "UserType",
        queryset: QuerySet,
        data_id: int,
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return queryset.filter(pk=user.id)


class ExposeEntity(ExposeObjectPlugin):
    """Expose the Entity model."""

    full_model_name = "flow.Entity"

    def can_create(self, user: "UserType", attributes: dict, data_id: int):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """

    def can_update(
        self, user: "UserType", model_instance: Data, model_data: dict, data_id: int
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

    def can_create(self, user: "UserType", attributes: dict, data_id: int):
        """Can user update the given model instance.

        :raises RuntimeError: if user does not have permissions to create the
            given model.
        """


class ExposeProcess(ExposeObjectPlugin):
    """Expose the Process model."""

    full_model_name = "flow.Process"

    def filter_objects(
        self,
        user: "UserType",
        queryset: QuerySet,
        data_id: int,
    ) -> QuerySet:
        """Filter the objects for the given user."""
        parent_ids = Data.objects.filter(pk=data_id).values_list("parents")
        processes_of_inputs = queryset.filter(data__in=parent_ids).distinct()
        return queryset.filter_for_user(user).distinct().union(processes_of_inputs)


class ExposeStorage(ExposeObjectPlugin):
    """Expose the Storage model."""

    full_model_name = "flow.Storage"

    def can_create(self, user: "UserType", attributes: dict, data_id: int):
        """Can user create the model with given attributes.

        :raises RuntimeError: when user does not have permissions to create
            the given model.
        """

    def can_update(
        self, user: "UserType", model_instance: Storage, model_data: dict, data_id: int
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
        self,
        user: "UserType",
        queryset: QuerySet,
        data_id: int,
    ) -> QuerySet:
        """Filter the objects for the given user."""
        return (
            queryset.filter_for_user(user)
            .distinct()
            .union(queryset.filter(contributor=user))
        )
