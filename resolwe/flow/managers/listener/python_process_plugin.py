"""Command handlers for python processes."""

import importlib
import logging
import os
from base64 import b64encode
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, TypeVar, Union
from zipfile import ZIP_STORED, ZipFile

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.postgres.fields.jsonb import JSONField as JSONFieldb
from django.db.models import ForeignKey, JSONField, ManyToManyField, Model, Q, Value
from django.db.models.functions import Concat

from resolwe.flow.executors import constants
from resolwe.flow.executors.socket_utils import Message, Response, retry
from resolwe.flow.managers.listener.permission_plugin import permission_manager
from resolwe.flow.models import Collection, Entity, Process
from resolwe.flow.models.base import UniqueSlugError
from resolwe.flow.models.utils import serialize_collection_relations
from resolwe.flow.utils import dict_dot
from resolwe.storage.connectors import connectors
from resolwe.storage.models import FileStorage
from resolwe.test.utils import is_testing

from .plugin import ListenerPlugin, listener_plugin_manager

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor

logger = logging.getLogger(__name__)
User = get_user_model()

# How many times to retry creating new object on slug collision error.
OBJECT_CREATE_RETRIES = 10

# How many objects to return in one chunk when iterating.
MAX_CHUNK_SIZE = 10000

PluginType = TypeVar("PluginType")


class PythonProcess(ListenerPlugin):
    """Handler methods for Python processes."""

    name = "PythonProcess plugin"
    plugin_manager = listener_plugin_manager

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

    def handle_iterate_objects(
        self,
        data_id: int,
        message: Message[Tuple[str, str, dict, list[str], list[str], int]],
        manager: "Processor",
    ) -> Response[Dict]:
        """Get a list of objects based on criteria.

        The list has at most 1000 entries.
        """
        # Sorting was added later. For compatibility reasons handle both
        # message types, remove the one without sorting ASAP.

        sorting: List[str] = []
        app_name, model_name, filters, sorting, attributes, offset = (
            message.message_data
        )
        full_model_name = f"{app_name}.{model_name}"
        model = apps.get_model(app_name, model_name)
        filtered_objects = self._permission_manager.filter_objects(
            manager.contributor(data_id),
            full_model_name,
            model.objects.filter(**filters),
            data_id,
        )
        number_of_objects = filtered_objects.count()
        to_return = {
            "number_of_matched_objects": number_of_objects,
            "chunk_size": MAX_CHUNK_SIZE,
            "starting_offset": offset,
            "objects": list(
                filtered_objects.order_by(*sorting).values_list(*attributes)[
                    offset : offset + MAX_CHUNK_SIZE
                ]
            ),
        }
        return message.respond_ok(to_return)

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

    def handle_set_entity_annotations(
        self,
        data_id: int,
        message: Message[tuple[int, dict[str, Any], bool]],
        manager: "Processor",
    ):
        """Handle update entity annotation request.

        The first part of the message is the id of the entity and the second one the
        mapping representing annotations. The keys in the are in the format
        f"{group_name}.{field_name}".
        """
        entity_id, annotations, update = message.message_data
        entity = Entity.objects.get(pk=entity_id)
        # Check that the user has the permissions to update the entity.
        self._permission_manager.can_update(
            manager.contributor(data_id), "flow.Entity", entity, {}, data_id
        )
        entity.update_annotations(annotations, manager.contributor(data_id), update)
        return message.respond_ok("OK")

    def handle_get_entity_annotations(
        self,
        data_id: int,
        message: Message[tuple[int, Optional[list[str]]]],
        manager: "Processor",
    ) -> Response[dict[str, Any]]:
        """Handle get annotations request.

        The first part of the message is the id of the entity and the second one the
        list representing annotations to retrieve. The annotations are in the format
        f"{group_name}.{field_name}".
        """
        entity_id, annotations = message.message_data
        # Check that the user has the permissions to read the entity.
        entity = self._permission_manager.filter_objects(
            manager.contributor(data_id),
            "flow.Entity",
            Entity.objects.filter(pk=entity_id),
            data_id,
        ).get()
        entity_annotations = entity.annotations
        if annotations is not None:
            if len(annotations) == 0:
                entity_annotations = entity_annotations.none()
            else:
                # Filter only requested annotations. Return only annotations matching group
                # and field name.
                annotation_filter = Q()
                for annotation in annotations:
                    group_name, field_name = annotation.split(".")
                    annotation_filter |= Q(
                        field__group__name=group_name, field__name=field_name
                    )
                entity_annotations.filter(annotation_filter)
        to_return = dict(
            entity_annotations.annotate(
                full_path=Concat("field__group__name", Value("."), "field__name")
            ).values_list("full_path", "_value__value")
        )
        return message.respond_ok(to_return)

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
        return message.respond_ok(User._meta.label)
