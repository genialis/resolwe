"""Python process Django Models."""
import json
import os
from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Type

from .communicator import communicator
from .fields import (
    BooleanField,
    DateTimeField,
    Field,
    IntegerField,
    JsonField,
    ListField,
    RelationDescriptor,
    StringField,
    fields_from_schema,
)

DATA_ID = int(os.getenv("DATA_ID", "-1"))
DATA_LOCAL_VOLUME = Path(os.environ.get("DATA_LOCAL_VOLUME", "/data_local"))
DATA_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data"))
DATA_ALL_VOLUME = Path(os.environ.get("DATA_ALL_VOLUME", "/data_all"))


class ModelField(Field):
    """Field representing a model.

    ForeignKey relation is modeled with this field.
    """

    field_type = "model"

    def __init__(self, full_model_name: str, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
        self.full_model_name = full_model_name
        self.app_name, self.related_model_name = full_model_name.split(".")
        self.label = full_model_name
        self.known_models = RegisteredModels()

    def to_python(self, value):
        """Return the python object representing the field."""
        if isinstance(value, Model):
            return value
        if isinstance(value, int):
            model_class = self.known_models.get_model(self.full_model_name)
            return model_class(value)
        return super().to_python(value)

    def to_output(self, value):
        """Convert the value to output format."""
        if isinstance(value, int):
            return value
        if isinstance(value, Model):
            return value.id
        if self.related_model_name == "Storage" and isinstance(value, str):
            return value
        return super().to_output(value)


FIELDS_MAP = {
    "AutoField": IntegerField,
    "VersionField": StringField,
    "CharField": StringField,
    "TextField": StringField,
    "DateTimeField": DateTimeField,
    "ForeignKey": ModelField,
    "ManyToManyField": ListField,
    "PositiveIntegerField": IntegerField,
    "PositiveSmallIntegerField": IntegerField,
    "ArrayField": Field,
    "JSONField": JsonField,
    "BigIntegerField": IntegerField,
    "BooleanField": BooleanField,
    "SlugField": StringField,
}


def hydrate_if_needed(value, model_instance, field_name, field):
    """Hydrate path if needed."""
    # Only hidrate paths on output of data objects that we are not processing.
    if (
        model_instance._model_name == "Data"
        and model_instance.id != DATA_ID
        and field_name == "output"
        and field.get_field_type()
        in ("basic:file", "list:basic:file", "basic:dir", "list:basic:dir")
    ):
        base_path = DATA_ALL_VOLUME / str(model_instance.location_id)
        if field.get_field_type().startswith("basic"):
            value.path = os.fspath(base_path / value.path)
        else:
            for entry in value:
                entry.path = os.fspath(base_path / entry.path)
    return value


class JSONDescriptor(MutableMapping[str, Any]):
    """JSON field."""

    def __init__(
        self,
        model: "Model",
        field_name: str,
        field_schema: List[Dict],
        read_only=False,
        cache: Optional[Dict[str, Any]] = None,
    ):
        """Initialization."""
        self._model = model
        self._model_name = self._model._model_name
        self._app_name = self._model._app_name
        self._field_name = field_name
        self._pk = self._model._pk
        self._cache: Dict[str, Any] = dict()
        self._read_only = read_only
        self._fields: Dict[str, Field] = dict()
        for field_name, field in fields_from_schema(field_schema).items():
            # JSON fields in schema are special since they represent the
            # storage model.
            if field.get_field_type() == "basic:json":
                field = ModelField(full_model_name="flow.Storage")
            field.contribute_to_class(self, self._fields, field_name)
        # Create cache if initial data is given.
        if cache is not None:
            self.refresh_cache(cache)

    def freeze(self):
        """Freeze fields."""
        self._read_only = True
        return self

    def refresh_cache(self, json_data: Optional[dict] = None):
        """Get the entire JSON field content from the server.

        TODO: read only required parts when Django supports it.
        """
        json_data = (
            json_data
            or communicator.get_model_fields(
                self._app_name, self._model_name, self._pk, [self._field_name]
            )[self._field_name]
        )
        for field_name, field in self._fields.items():
            if field_name in json_data:
                self._cache[field_name] = hydrate_if_needed(
                    field.clean(json_data[field_name]),
                    self._model,
                    self._field_name,
                    field,
                )

    def __getattr__(self, key):
        """Allow dot syntax."""
        return self.__getitem__(key)

    def __setattr__(self, key, value):
        """Set the value for the given key."""
        if key.startswith("_"):
            super().__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def __getitem__(self, key):
        """Get the value for the given key."""
        if self._fields is not None and key not in self._fields:
            raise AttributeError(f"Value {key} must be in the fields")
        if key not in self._cache:
            self.refresh_cache()
        # Return None even if key is not known.
        return self._cache.get(key)

    def __setitem__(self, key, value):
        """Set the value for the given key."""
        if self._read_only:
            raise ValueError("Cannot change read-only mapping.")
        if key not in self._fields:
            raise AttributeError(f"No field named {key}")
        to_output = None
        field = self._fields[key]
        if getattr(field, "related_model_name", None) == "Storage":
            # Check if JSON must be read from the file.
            if isinstance(value, str):
                json_file = Path(value)
                if json_file.is_file():
                    value = json.loads(json_file.read_text())
            storage = getattr(self, key)
            if storage is None:
                storage = Storage.create(
                    json=value,
                    name="Storage for data id {}".format(self._pk),
                    contributor=self._model.contributor,
                )
                storage.data += [self._model]
                value = storage
                to_output = storage.id
            else:
                storage.json = value
        else:
            value = field.clean(value)
            to_output = field.to_output(value)

        if to_output is not None:
            communicator.update_model_fields(
                self._app_name,
                self._model_name,
                self._pk,
                {self._field_name: {key: to_output}},
            )
        self._cache[key] = value

    def __len__(self) -> int:
        """Return the number of known fields."""
        if self._fields is not None:
            return len(self._fields)
        else:
            return len(self._cache)

    def __delitem__(self, name):
        """Delete the item from JSON field."""
        raise NotImplementedError("Delete is not implemented")

    def __iter__(self):
        """Return iterator."""
        return iter(self._cache)

    def __str__(self):
        """Return string representation."""
        return f"JSONDescriptor({self._model}, {self._field_name})"


class ModelMetaclass(type):
    """Construct fields for the Django class."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        """Create new Model class."""
        model: Type[Model] = type.__new__(mcs, name, bases, namespace)
        fields: Dict[str, Field] = {}
        if name != "Model" and communicator is not None:
            app_name = model._app_name
            fields_details = communicator.get_model_fields_details(app_name, name)
            for field_name in fields_details:
                field_type, required, related_model_name = fields_details[field_name]
                if field_type in FIELDS_MAP:
                    kwargs = {"required": required}
                    if field_type == "ForeignKey":
                        id_field = IntegerField()
                        id_field.contribute_to_class(model, fields, f"{field_name}_id")
                        setattr(model, f"{field_name}_id", id_field)
                        kwargs["full_model_name"] = related_model_name
                    if field_type == "ManyToManyField":
                        kwargs["inner"] = ModelField(related_model_name)
                    field_class = FIELDS_MAP[field_type]
                    field = field_class(**kwargs)
                    field.contribute_to_class(model, fields, field_name)
                    setattr(model, field_name, field)

            model.fields = fields
        return model


class JSONModelEncoder(json.JSONEncoder):
    """Encode Model to JSON.

    When instance of Model class is serialized to JSON it should be serialized
    to its id property.
    """

    def default(self, o: Any):
        """Override default method."""
        if isinstance(o, Model):
            return o.id
        return json.JSONEncoder.default(self, o)


class Model(metaclass=ModelMetaclass):
    """Base django model."""

    _app_name = "App name"
    _model_name = "Model name"
    # A default list of extra fields that are returned on filter call.
    _filter_response_fields = []

    def __init__(self, pk: int):
        """Initialization."""
        self._pk = pk
        self._cache: Dict[str, Any] = {"id": pk}

    @property
    def full_model_name(self):
        """Return the full model name."""
        return f"{self._app_name}.{self._model_name}"

    @classmethod
    def filter(cls, **filters: Dict[str, Any]) -> List["Model"]:
        """Return a list of all objects that fit criteria."""
        # Make sure attributes have 'id' in the first place.
        attributes = filters.pop("__fields", None)
        attributes = attributes or cls._filter_response_fields
        attributes = ["id"] + [
            attribute for attribute in attributes if attribute != "id"
        ]

        objects = communicator.filter_objects(
            cls._app_name, cls._model_name, filters, attributes
        )
        models = []
        for entry in objects:
            model = cls(entry[0])
            for field_name, value in zip(attributes[1:], entry[1:]):
                field = model.fields[field_name]
                model._cache[field_name] = field.clean(value)
            models.append(model)
        return models

    @classmethod
    def exists(cls, **filters: Dict[str, Any]) -> List[int]:
        """Check if objects that fit criteria exists.

        If no such object exists empty list is returned.
        Else list of ids that fit the criteria is returned.
        """
        return [
            e[0]
            for e in communicator.filter_objects(
                cls._app_name, cls._model_name, filters, ["id"]
            )
        ]

    @classmethod
    def get(cls, **filters: Dict[str, Any]) -> "Model":
        """Get a single model based on filters.

        :raises RuntimeError: when different than one objects match the given
            criteria.
        """
        pks = communicator.filter_objects(
            cls._app_name, cls._model_name, filters, ["id"]
        )
        if len(pks) != 1:
            raise RuntimeError("Exactly one object should match the given criteria.")
        return cls(pks[0][0])

    @classmethod
    def create(cls, **object_data: Dict[str, Any]) -> int:
        """Create object with the given data.

        If creation was successfull return its id.
        """
        mappings = []
        for field_name in object_data:
            field = cls.fields.get(field_name)
            if field is not None and field.get_field_type() == "model":
                mappings.append(
                    (field_name, f"{field_name}_id", object_data[field_name].id)
                )
        for old_name, new_name, new_value in mappings:
            del object_data[old_name]
            object_data[new_name] = new_value
        communicator.encoder = JSONModelEncoder
        return cls(
            communicator.create_object(cls._app_name, cls._model_name, object_data)
        )

    def __str__(self):
        """Return a string representation."""
        return f"{self._model_name}(pk={self._pk})"

    def _set_field_data(self, field: Field, value: Any):
        """Set the value of the field."""
        self._cache[field.name] = value
        communicator.update_model_fields(
            self._app_name,
            self._model_name,
            self._pk,
            {field.name: field.to_output(value)},
        )

    def _get_field_data(self, field: Field) -> Any:
        """Get data for the given field.

        It is used by the fields (descriptors) to retrieve the data from the
        server. The cache is used if applicable.
        """
        if field.name not in self._cache:
            result = communicator.get_model_fields(
                self._app_name, self._model_name, self._pk, [field.name]
            )
            if len(result) > 1:
                result = [e[field.name] for e in result]
            else:
                result = result[field.name]
            self._cache[field.name] = field.clean(result)
        return self._cache[field.name]

    @classmethod
    def __init_subclass__(cls: Type["Model"], **kwargs):
        """Register class."""
        super().__init_subclass__(**kwargs)
        RegisteredModels().register_model(cls)


class RegisteredModels:
    """Registered Python process models."""

    #  A single instance of this class.
    __instance = None
    _known_models: Dict[str, Type["Model"]] = dict()

    @classmethod
    def __new__(cls, *args, **kwargs):
        """Create or return singleton."""
        if RegisteredModels.__instance is None:
            RegisteredModels.__instance = super().__new__(cls)

        return RegisteredModels.__instance

    def get_model(self, full_model_name: str) -> Type["Model"]:
        """Get the registered model."""
        return self._known_models.get(full_model_name, Model)

    def register_model(self, model_class: Type["Model"]):
        """Add new model class.

        :raises AssertionError: when model is already registered.
        """
        full_model_name = f"{model_class._app_name}.{model_class._model_name}"
        assert (
            full_model_name not in self._known_models
        ), f"Model named {full_model_name} already registered."
        self._known_models[full_model_name] = model_class


class Process(Model):
    """Process model."""

    _app_name = "flow"
    _model_name = "Process"


class Entity(Model):
    """Entity model."""

    _app_name = "flow"
    _model_name = "Entity"


class Storage(Model):
    """Storage model."""

    _app_name = "flow"
    _model_name = "Storage"


class User(Model):
    """User model."""

    _app_name = "auth"
    _model_name = "User"


class Collection(Model):
    """Collection model."""

    _app_name = "flow"
    _model_name = "Collection"

    @property
    def relations(self):
        """Get relation."""
        if "__relations" not in self._cache:
            self._cache["__relations"] = communicator.get_relations(self.id)
        return self._cache["__relations"]


class Data(Model):
    """Data object."""

    _app_name = "flow"
    _model_name = "Data"

    @classmethod
    def from_slug(cls, slug: str) -> "Data":
        """Get Data object from slug."""
        return Data(communicator.get_data_by_slug(slug))

    @property
    def type(self) -> str:
        """Get the process type for this data object.

        DEPRECATED: compatibility with old DataDescriptor class.
        """
        return self.process.type

    @property
    def entity_name(self):
        """Entity name.

        Causes error when entity is not defined.

        DEPRECATED: compatibility with old DataDescriptor class.
        """
        return self.entity.name

    @property
    def relations(self):
        """Get relations for this data object in its collection."""
        relations = set()
        for relation in self.collection.relations:
            for partition in relation["partitions"]:
                if partition["entity_id"] == self.entity.id:
                    relations.add(RelationDescriptor.from_dict(relation))
        return list(relations)
