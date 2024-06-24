"""Resolwe models validation utils."""

import json
import re
from pathlib import Path

import jsonschema
from django.contrib.staticfiles import finders
from django.core.exceptions import ValidationError

from resolwe.flow.utils import dict_dot, iterate_dict, iterate_fields, iterate_schema


class DirtyError(ValidationError):
    """Error raised when required fields missing."""


def validation_schema(name):
    """Return json schema for json validation."""
    schemas = {
        "processor": "processSchema.json",
        "descriptor": "descriptorSchema.json",
        "field": "fieldSchema.json",
        "type": "typeSchema.json",
        "process_resources": "resourcesSchema.json",
    }

    if name not in schemas:
        raise ValueError()

    field_schema_file = finders.find("flow/{}".format(schemas["field"]), all=True)[0]
    with open(field_schema_file, "r") as fn:
        field_schema = fn.read()

    if name == "field":
        return json.loads(field_schema.replace("{{PARENT}}", ""))

    schema_file = finders.find("flow/{}".format(schemas[name]), all=True)[0]
    with open(schema_file, "r") as fn:
        schema = fn.read()

    return json.loads(
        schema.replace("{{FIELD}}", field_schema).replace("{{PARENT}}", "/field")
    )


TYPE_SCHEMA = validation_schema("type")


def validate_schema(
    instance, schema, test_required=True, data_location=None, skip_missing_data=False
):
    """Check if DictField values are consistent with our data types.

    Perform basic JSON schema validation and our custom validations:

      * check that required fields are given (if `test_required` is set
        to ``True``)
      * check if ``basic:file:`` and ``list:basic:file`` fields match
        regex given in schema (only if ``validate_regex`` is defined in
        schema for coresponding fields) and exists (only if
        ``data_location`` is given)
      * check if directories referenced in ``basic:dir:`` and
        ``list:basic:dir``fields exist (only if ``data_location`` is
        given)
      * check that referenced ``Data`` objects (in ``data:<data_type>``
        and  ``list:data:<data_type>`` fields) exists and are of type
        ``<data_type>``
      * check that referenced ``Storage`` objects (in ``basic:json``
        fields) exists

    :param list instance: Instance to be validated
    :param list schema: Schema for validation
    :param bool test_required: Flag for testing if all required fields
        are present. It is usefule if validation is run before ``Data``
        object is finished and there are some field stil missing
        (default: ``False``)
    :param :class:`~resolwe.storage.models.FileStorage` data_location:
        data location used for checking if files and directories exist
        (default: ``None``)
    :param bool skip_missing_data: Don't raise an error if referenced
        ``Data`` object does not exist
    :rtype: None
    :raises ValidationError: if ``instance`` doesn't match schema
        defined in ``schema``

    """
    from ..storage import Storage  # Prevent circular import.

    path_prefix = None
    if data_location:
        path_prefix = Path(data_location.get_path())

    def validate_refs(field):
        """Validate reference paths."""
        for ref_filename in field.get("refs", []):
            ref_path: Path = path_prefix / ref_filename
            file_exists = ref_path.exists()
            if not file_exists:
                raise ValidationError(
                    "Path referenced in `refs` ({}) does not exist.".format(ref_path)
                )
            if not (ref_path.is_file() or ref_path.is_dir()):
                raise ValidationError(
                    "Path referenced in `refs` ({}) is neither a file or directory.".format(
                        ref_path
                    )
                )

    def validate_file(field, regex):
        """Validate file name (and check that it exists)."""
        filename = field["file"]
        if regex and not re.search(regex, filename):
            raise ValidationError(
                "File name {} does not match regex {}".format(filename, regex)
            )

        if path_prefix:
            path: Path = path_prefix / filename
            if not path.exists():
                raise ValidationError(
                    "Referenced path ({}) does not exist.".format(path)
                )
            if not path.is_file():
                raise ValidationError(
                    "Referenced path ({}) is not a file.".format(path)
                )

            validate_refs(field)

    def validate_dir(field):
        """Check that dirs and referenced files exists."""
        dirname = field["dir"]

        if path_prefix:
            path: Path = path_prefix / dirname
            if not path.exists():
                raise ValidationError(
                    "Referenced path ({}) does not exist.".format(path)
                )
            if not path.is_dir():
                raise ValidationError(
                    "Referenced path ({}) is not a directory.".format(path)
                )

            validate_refs(field)

    def validate_data(data_pk, type_):
        """Check that `Data` objects exist and is of right type."""
        from ..data import Data  # prevent circular import

        data_qs = Data.objects.filter(pk=data_pk).values("process__type")
        if not data_qs.exists():
            if skip_missing_data:
                return

            raise ValidationError(
                "Referenced `Data` object does not exist (id:{})".format(data_pk)
            )
        data = data_qs.first()
        if not data["process__type"].startswith(type_):
            raise ValidationError(
                "Data object of type `{}` is required, but type `{}` is given. "
                "(id:{})".format(type_, data["process__type"], data_pk)
            )

    def validate_range(value, interval, name):
        """Check that given value is inside the specified range."""
        if not interval:
            return

        if value < interval[0] or value > interval[1]:
            raise ValidationError(
                "Value of field '{}' is out of range. It should be between {} and {}.".format(
                    name, interval[0], interval[1]
                )
            )

    is_dirty = False
    dirty_fields = []
    for _schema, _fields, _ in iterate_schema(instance, schema):
        name = _schema["name"]
        is_required = _schema.get("required", True)

        if test_required and is_required and name not in _fields:
            is_dirty = True
            dirty_fields.append(name)

        if name in _fields:
            field = _fields[name]
            type_ = _schema.get("type", "")

            # Treat None as if the field is missing.
            if not is_required and field is None:
                continue

            try:
                jsonschema.validate([{"type": type_, "value": field}], TYPE_SCHEMA)
            except jsonschema.exceptions.ValidationError as ex:
                raise ValidationError(ex.message)

            choices = [choice["value"] for choice in _schema.get("choices", [])]
            allow_custom_choice = _schema.get("allow_custom_choice", False)
            if choices and not allow_custom_choice and field not in choices:
                raise ValidationError(
                    "Value of field '{}' must match one of predefined choices. "
                    "Current value: {}".format(name, field)
                )

            if type_ == "basic:file:":
                validate_file(field, _schema.get("validate_regex"))

            elif type_ == "list:basic:file:":
                for obj in field:
                    validate_file(obj, _schema.get("validate_regex"))

            elif type_ == "basic:dir:":
                validate_dir(field)

            elif type_ == "list:basic:dir:":
                for obj in field:
                    validate_dir(obj)

            elif (
                type_ == "basic:json:" and not Storage.objects.filter(pk=field).exists()
            ):
                raise ValidationError(
                    "Referenced `Storage` object does not exist (id:{})".format(field)
                )

            elif type_.startswith("data:"):
                validate_data(field, type_)

            elif type_.startswith("list:data:"):
                for data_id in field:
                    validate_data(data_id, type_[5:])  # remove `list:` from type

            elif type_ == "basic:integer:" or type_ == "basic:decimal:":
                validate_range(field, _schema.get("range"), name)

            elif type_ == "list:basic:integer:" or type_ == "list:basic:decimal:":
                for obj in field:
                    validate_range(obj, _schema.get("range"), name)

    try:
        # Check that schema definitions exist for all fields
        for _, _ in iterate_fields(instance, schema):
            pass
    except KeyError as ex:
        raise ValidationError(str(ex))

    if is_dirty:
        dirty_fields = ['"{}"'.format(field) for field in dirty_fields]
        raise DirtyError(
            "Required fields {} not given.".format(", ".join(dirty_fields))
        )


def validate_data_object(data, skip_missing_data=False):
    """Validate data object.

    Data object is validated only when worker is done with processing.
    """
    validate_schema(
        data.input, data.process.input_schema, skip_missing_data=skip_missing_data
    )
    validate_schema(data.output, data.process.output_schema)


def validate_process_subtype(supertype_name, supertype, subtype_name, subtype):
    """Perform process subtype validation.

    :param supertype_name: Supertype name
    :param supertype: Supertype schema
    :param subtype_name: Subtype name
    :param subtype: Subtype schema
    :return: A list of validation error strings
    """
    errors = []
    for item in supertype:
        # Ensure that the item exists in subtype and has the same schema.
        for subitem in subtype:
            if item["name"] != subitem["name"]:
                continue

            for key in set(item.keys()) | set(subitem.keys()):
                if key in ("label", "description"):
                    # Label and description can differ.
                    continue
                elif key == "required":
                    # A non-required item can be made required in subtype, but not the
                    # other way around.
                    item_required = item.get("required", True)
                    subitem_required = subitem.get("required", False)

                    if item_required and not subitem_required:
                        errors.append(
                            "Field '{}' is marked as required in '{}' and optional in '{}'.".format(
                                item["name"], supertype_name, subtype_name
                            )
                        )
                elif item.get(key, None) != subitem.get(key, None):
                    errors.append(
                        "Schema for field '{}' in type '{}' does not match supertype '{}'.".format(
                            item["name"], subtype_name, supertype_name
                        )
                    )

            break
        else:
            errors.append(
                "Schema for type '{}' is missing supertype '{}' field '{}'.".format(
                    subtype_name, supertype_name, item["name"]
                )
            )

    return errors


def validate_process_types(queryset=None):
    """Perform process type validation.

    :param queryset: Optional process queryset to validate
    :return: A list of validation error strings
    """
    if not queryset:
        from ..process import Process

        queryset = Process.objects.all()

    processes = {}
    for process in queryset:
        dict_dot(
            processes,
            process.type.replace(":", ".") + "__schema__",
            process.output_schema,
        )

    errors = []
    for path, key, value in iterate_dict(
        processes, exclude=lambda key, value: key == "__schema__"
    ):
        if "__schema__" not in value:
            continue

        # Validate with any parent types.
        for length in range(len(path), 0, -1):
            parent_type = ".".join(path[:length] + ["__schema__"])
            try:
                parent_schema = dict_dot(processes, parent_type)
            except KeyError:
                continue

            errors += validate_process_subtype(
                supertype_name=":".join(path[:length]),
                supertype=parent_schema,
                subtype_name=":".join(path + [key]),
                subtype=value["__schema__"],
            )

    return errors
