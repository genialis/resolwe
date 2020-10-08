"""Python process Django Models."""
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
    """Model field."""

    field_type = "model"

    def __init__(self, related_model_name: str, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
        self.related_model_name = related_model_name

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
            return value._pk
        return None

    @property
    def _descriptor_field_name(self):
        """Get descriptor field name."""
        return f"{self.name}"


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
                if field.get_field_type() == "basic:json":
                    # This is actually a storage field.
                    field = ModelField(related_model_name="Storage")
                field.contribute_to_class(self, self._fields, field_name)
        if cache is not None:
            self.refresh_cache(cache)
        print(f"Init: {self}")

    def freeze(self):
        """Freeze fields."""
        self._read_only = True
        return self

    def refresh_cache(self, json_data: Optional[dict] = None):
        """Get the entire JSON field content from the server."""
        json_data = (
            json_data
            or communicator.get_model_fields(
                self._model_name, self._pk, [self._field_name]
            )[self._field_name]
        )
        if self._fields is not None:
            for field_name, field in self._fields.items():
                if field_name in json_data:
                    print(
                        f"Instatianing field {field_name} of type {field.get_field_type()} with data {json_data[field_name]}"
                    )
                    self._cache[field_name] = field.clean(json_data[field_name])
                    # TODO: hydrate nicer!!! Do not use hardcoded values (data_all)
                    if (
                        field.get_field_type() == "basic:file"
                        and self._model_name == "Data"
                    ):
                        if self._pk != DATA_ID:
                            base_path = DATA_ALL_VOLUME / str(self._model.location_id)
                            self._cache[
                                field_name
                            ].path = f"{base_path}/{self._cache[field_name].path}"
                    elif (
                        field.get_field_type() == "list:basic:file"
                        and self._model_name == "Data"
                    ):
                        if self._pk != DATA_ID:
                            base_path = DATA_ALL_VOLUME / str(self._model.location_id)
                            for entry in self._cache[field_name]:
                                entry.path = f"{base_path}/{self._model.location_id}/{entry.path}"
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
        print(f"Get {key} of JSONDescriptor {self._field_name}")
        print(f"Model name: {self._model_name}")
        print(f"Model instance: {self._model}")
        print(f"Model id: {self._model._pk}")
        print(f"Known fields: {self._fields}")
        if self._fields is not None and key not in self._fields:
            raise AttributeError(f"Value {key} must be in the fields")
        if key not in self._cache:
            self.refresh_cache()
        # Return None even if key is not known.
        print(f"Got: {self._cache.get(key)}")
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
                if name == "Data":
                    print(f"Processing field: ", {field_name})
                    print(f"Required: {required}")
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
                    print("Required: ", field.required)
                else:
                    print(f"Field type {field_type} not in mapping")

            model.fields = fields
            model.objects = ObjectsManager(model, name)

        return model


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
        return communicator.create_object(self._model_name, object_data)


class Process(Model):
    """Process model."""

    def __init__(self, object_id: int):
        """Initialization."""
        print(f"Creating {self.__class__.__name__} object with id {object_id}")
        super().__init__(self.__class__.__name__, object_id)


class Entity(Model):
    """Entity model."""

    def __init__(self, object_id: int):
        """Initialization."""
        print(f"Creating {self.__class__.__name__} object with id {object_id}")
        super().__init__(self.__class__.__name__, object_id)


class Storage(Model):
    """Storage model."""

    def __init__(self, object_id: int):
        """Initialization."""
        print(f"Creating {self.__class__.__name__} object with id {object_id}")
        super().__init__(self.__class__.__name__, object_id)


class Collection(Model):
    """Collection model."""

    def __init__(self, object_id: int):
        """Initialization."""
        print(f"Creating {self.__class__.__name__} object with id {object_id}")
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
    def output(self) -> JSONDescriptor:
        """Output JSON field."""
        print("IN PROPERTY!!!!!!!!!!!!!!!!!!!!!!!!!")
        if "output" not in self._cache:
            field = JSONDescriptor(
                self,
                "output",
                self.process.output_schema,
            )
            self._cache["output"] = field
        return self._cache["output"]

    @property
    def descriptor(self) -> JSONDescriptor:
        """Descriptor JSON field."""
        if "descriptor" not in self._cache:
            field = JSONDescriptor(
                self,
                "descriptor",
                self.process.descriptor_schema,
            )
            self._cache["output"] = field
        return self._cache["output"]

    @property
    def input(self) -> JSONDescriptor:
        """Input JSON field."""
        if "input" not in self._cache:
            field = JSONDescriptor(
                self,
                "input",
                self.process.input_schema,
                read_only=True,
            )
            self._cache["input"] = field
        return self._cache["input"]

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

    @property
    def storages(self) -> List[Storage]:
        """Get storages belonging to the data object."""
        return super().storages()

    # @storage.setter
    # def storage(self, value: str):
    #     """
    #     Handle storage JSON field differently.

    #     The value is a path to the file with the JSON content. The file is
    #     copied to the data directory and listener reads its content and stores
    #     it to the storage field.

    #     TODO: is this really desired??
    #     """
    #     source = Path(value)
    #     if source.is_file():
    #         destination = DATA_VOLUME / source
    #         shutil.copy2(source, destination)
    #         setattr(super(), "storage", value)
