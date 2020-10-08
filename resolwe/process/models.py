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
    copy_file_or_dir,
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

    def __init__(self, related_model_name: str, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
        self.related_model_name = related_model_name
        self.label = related_model_name

    def to_python(self, value):
        """Return the python object representing the field."""
        if isinstance(value, Model):
            return value
        if isinstance(value, int):
            known = globals()
            if self.related_model_name in known:
                return known[self.related_model_name](value)
            else:
                return Model(self.related_model_name, value)
        return super().to_python(value)

    def to_output(self, value):
        """Convert the value to output format."""
        if isinstance(value, int):
            return value
        if isinstance(value, Model):
            return value.id
        if self.related_model_name == "Storage" and isinstance(value, str):
            if Path(value).is_file():
                copy_file_or_dir([value])
        return super().to_output(value)


FIELDS_MAP = {
    "AutoField": IntegerField,
    "VersionField": StringField,
    "CharField": StringField,
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
        field_schema: Optional[List[Dict]] = None,
        read_only=False,
        cache: Optional[Dict[str, Any]] = None,
    ):
        """Initialization."""
        self._model = model
        self._model_name = self._model._model_name
        self._field_name = field_name
        self._pk = self._model._pk
        self._cache: Dict[str, Any] = dict()
        self._fields: Optional[Dict[str, Field]] = None
        self._read_only = read_only
        if field_schema is not None:
            self._fields = dict()
            for field_name, field in fields_from_schema(field_schema).items():
                # JSON fields in schema are actually specias since they
                # represent the storage model.
                if field.get_field_type() == "basic:json":
                    field = ModelField(related_model_name="Storage")
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
                self._model_name, self._pk, [self._field_name]
            )[self._field_name]
        )
        if self._fields is not None:
            for field_name, field in self._fields.items():
                if field_name in json_data:
                    self._cache[field_name] = hydrate_if_needed(
                        field.clean(json_data[field_name]),
                        self._model,
                        self._field_name,
                        field,
                    )
        else:
            self._cache = json_data

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

        to_output = value
        if self._fields is not None:
            if key not in self._fields:
                raise AttributeError(f"No field named {key}")
            value = self._fields[key].clean(value)
            to_output = self._fields[key].to_output(value)
        communicator.update_model_fields(
            self._model_name, self._pk, {self._field_name: {key: to_output}}
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
            fields_details = communicator.get_model_fields_details(name)
            for field_name in fields_details:
                field_type, required, related_model_name = fields_details[field_name]
                if field_type in FIELDS_MAP:
                    kwargs = {"required": required}
                    if field_type == "ForeignKey":
                        id_field = IntegerField()
                        id_field.contribute_to_class(model, fields, f"{field_name}_id")
                        setattr(model, f"{field_name}_id", id_field)
                        kwargs["related_model_name"] = related_model_name
                    if field_type == "ManyToManyField":
                        kwargs["inner"] = ModelField(related_model_name)
                    field_class = FIELDS_MAP[field_type]
                    field = field_class(**kwargs)
                    field.contribute_to_class(model, fields, field_name)
                    setattr(model, field_name, field)

            model.fields = fields
            model.objects = ObjectsManager(model, name)

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

    def __init__(self, model_name: str, pk: int):
        """Initialization."""
        self._pk = pk
        self._model_name = model_name
        self._cache: Dict[str, Any] = {"id": pk}

    def __str__(self):
        """Return a string representation."""
        return f"{self._model_name}(pk={self._pk})"

    def _set_field_data(self, field: Field, value: Any):
        """Set the value of the field."""
        self._cache[field.name] = value
        communicator.update_model_fields(
            self._model_name, self._pk, {field.name: field.to_output(value)}
        )

    def _get_field_data(self, field: Field) -> Any:
        """Get data for the given field.

        It is used by the fields (descriptors) to retrieve the data from the
        server. The cache is used if applicable.
        """
        if field.name not in self._cache:
            result = communicator.get_model_fields(
                self._model_name, self._pk, [field.name]
            )
            if len(result) > 1:
                result = [e[field.name] for e in result]
            else:
                result = result[field.name]
            self._cache[field.name] = field.clean(result)
        return self._cache[field.name]


class ObjectsManager:
    """Class for filtering and creating new objects."""

    def __init__(self, model: Type[Model], model_name: str):
        """Initialize."""
        self._model = model
        self._model_name = model_name

    def filter(self, **filters: Dict[str, Any]) -> List[Model]:
        """Create a filter of all objects that fit criteria."""
        pks = communicator.filter_objects(self._model_name, filters)
        return [self._model(pk) for pk in pks]

    def exists(self, **filters: Dict[str, Any]) -> List[int]:
        """Check if objects that fit criteria exists.

        If no such object exists empty list is returned.
        Else list of ids that fit the criteria is returned.
        """
        return communicator.filter_objects(self._model_name, filters)

    def get(self, **filters: Dict[str, Any]) -> Model:
        """Get a single model based on filters.

        :raises RuntimeError: when different than one objects match the given
            criteria.
        """
        pks = communicator.filter_objects(self._model_name, filters)
        if len(pks) != 1:
            raise RuntimeError("Not only one object meats the given criteria.")
        return self._model(pks[0])

    def create(self, **object_data: Dict[str, Any]) -> int:
        """Create object with the given data.

        If creation was successfull return its id.
        """
        mappings = []
        for field_name in object_data:
            field = self._model.fields.get(field_name)
            if field is not None and field.get_field_type() == "model":
                mappings.append(
                    (field_name, f"{field_name}_id", object_data[field_name].id)
                )
        for old_name, new_name, new_value in mappings:
            del object_data[old_name]
            object_data[new_name] = new_value
        print("Creating object: ", object_data)
        communicator.encoder = JSONModelEncoder
        return self._model(communicator.create_object(self._model_name, object_data))


class Process(Model):
    """Process model."""

    def __init__(self, object_id: int):
        """Initialization."""
        super().__init__(self.__class__.__name__, object_id)


class Entity(Model):
    """Entity model."""

    def __init__(self, object_id: int):
        """Initialization."""
        super().__init__(self.__class__.__name__, object_id)


class Storage(Model):
    """Storage model."""

    def __init__(self, object_id: int):
        """Initialization."""
        super().__init__(self.__class__.__name__, object_id)


class Collection(Model):
    """Collection model."""

    def __init__(self, object_id: int):
        """Initialization."""
        super().__init__(self.__class__.__name__, object_id)

    @property
    def relations(self):
        """Get relation."""
        if "__relations" not in self._cache:
            self._cache["__relations"] = communicator.get_relations(self.id)
        return self._cache["__relations"]


class Data(Model):
    """Data object."""

    def __init__(self, object_id: int):
        """Initialization."""
        super().__init__(self.__class__.__name__, object_id)

    @classmethod
    def from_slug(self, slug: str) -> "Data":
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
