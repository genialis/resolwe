"""Resolwe models hydrate utils."""
import copy
import os
import re
from pathlib import Path

from django.core.exceptions import ValidationError

from resolwe.flow.utils import iterate_fields


def _hydrate_values(output, output_schema, data):
    """Hydrate basic:file and basic:json values.

    Find fields with basic:file type and assign a full path to the file.
    Find fields with basic:json type and assign a JSON object from storage.

    """

    def hydrate_path(file_name):
        """Hydrate file paths."""
        from resolwe.flow.managers import manager

        class HydratedPath(str):
            """String wrapper, which also stores the original filename."""

            __slots__ = ("data_id", "file_name")

            def __new__(cls, value=""):
                """Initialize hydrated path."""
                hydrated = str.__new__(cls, value)
                hydrated.data_id = data.id
                hydrated.file_name = file_name
                return hydrated

        return HydratedPath(manager.get_executor().resolve_data_path(data, file_name))

    def hydrate_storage(storage_id):
        """Hydrate storage fields."""
        from ..storage import LazyStorageJSON  # Prevent circular import.

        return LazyStorageJSON(pk=storage_id)

    for field_schema, fields in iterate_fields(output, output_schema):
        name = field_schema["name"]
        value = fields[name]
        if "type" in field_schema:
            if field_schema["type"].startswith("basic:file:"):
                value["file"] = hydrate_path(value["file"])
                value["refs"] = [hydrate_path(ref) for ref in value.get("refs", [])]

            elif field_schema["type"].startswith("list:basic:file:"):
                for obj in value:
                    obj["file"] = hydrate_path(obj["file"])
                    obj["refs"] = [hydrate_path(ref) for ref in obj.get("refs", [])]

            if field_schema["type"].startswith("basic:dir:"):
                value["dir"] = hydrate_path(value["dir"])
                value["refs"] = [hydrate_path(ref) for ref in value.get("refs", [])]

            elif field_schema["type"].startswith("list:basic:dir:"):
                for obj in value:
                    obj["dir"] = hydrate_path(obj["dir"])
                    obj["refs"] = [hydrate_path(ref) for ref in obj.get("refs", [])]

            elif field_schema["type"].startswith("basic:json:"):
                fields[name] = hydrate_storage(value)

            elif field_schema["type"].startswith("list:basic:json:"):
                fields[name] = [hydrate_storage(storage_id) for storage_id in value]


def hydrate_input_references(input_, input_schema, hydrate_values=True):
    """Hydrate ``input_`` with linked data.

    Find fields with complex data:<...> types in ``input_``.
    Assign an output of corresponding data object to those fields.

    """
    from ..data import Data  # prevent circular import

    for field_schema, fields in iterate_fields(input_, input_schema):
        name = field_schema["name"]
        value = fields[name]
        if "type" in field_schema:
            if field_schema["type"].startswith("data:"):
                if value is None:
                    continue

                try:
                    data = Data.objects.get(id=value)
                except Data.DoesNotExist:
                    fields[name] = {}
                    continue

                output = copy.deepcopy(data.output)
                hydrate_input_references(output, data.process.output_schema)
                if hydrate_values:
                    _hydrate_values(output, data.process.output_schema, data)
                output["__id"] = data.id
                output["__type"] = data.process.type
                output["__descriptor"] = data.descriptor
                output["__name"] = getattr(data, "name", None)
                output["__entity_id"] = getattr(data.entity, "id", None)
                output["__entity_name"] = getattr(data.entity, "name", None)
                output["__output_schema"] = data.process.output_schema

                fields[name] = output

            elif field_schema["type"].startswith("list:data:"):
                outputs = []
                for val in value:
                    if val is None:
                        continue

                    try:
                        data = Data.objects.get(id=val)
                    except Data.DoesNotExist:
                        outputs.append({})
                        continue

                    output = copy.deepcopy(data.output)
                    hydrate_input_references(output, data.process.output_schema)
                    if hydrate_values:
                        _hydrate_values(output, data.process.output_schema, data)

                    output["__id"] = data.id
                    output["__type"] = data.process.type
                    output["__descriptor"] = data.descriptor
                    output["__name"] = getattr(data, "name", None)
                    output["__entity_id"] = getattr(data.entity, "id", None)
                    output["__entity_name"] = getattr(data.entity, "name", None)
                    output["__output_schema"] = data.process.output_schema

                    outputs.append(output)

                fields[name] = outputs


def hydrate_input_uploads(input_, input_schema, hydrate_values=True):
    """Hydrate input basic:upload types with upload location.

    Find basic:upload fields in input.
    Add the upload location for relative paths.

    """
    from resolwe.flow.managers import manager

    files = []
    for field_schema, fields in iterate_fields(input_, input_schema):
        name = field_schema["name"]
        value = fields[name]
        if "type" in field_schema:
            if field_schema["type"] == "basic:file:":
                files.append(value)

            elif field_schema["type"] == "list:basic:file:":
                files.extend(value)

    urlregex = re.compile(
        r"^(https?|ftp|s3)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]"
    )
    for value in files:
        if "file_temp" in value:
            if isinstance(value["file_temp"], str):
                # If file_temp not url, hydrate path.
                if not urlregex.search(value["file_temp"]):
                    value["file_temp"] = manager.get_executor().resolve_upload_path(
                        value["file_temp"]
                    )
            else:
                # Something very strange happened.
                value["file_temp"] = "Invalid value for file_temp in DB"


def hydrate_size(data, force=False):
    """Add file and dir sizes.

    Add sizes to ``basic:file:``, ``list:basic:file``, ``basic:dir:``
    and ``list:basic:dir:`` fields.

    ``force`` parameter is used to recompute file sizes also on objects
    that already have these values, e.g. in migrations.
    """
    from ..data import Data  # prevent circular import

    def get_dir_size(path):
        """Get directory size."""
        return sum(
            file_.stat().st_size for file_ in Path(path).rglob("*") if file_.is_file()
        )

    def get_refs_size(obj, obj_path):
        """Calculate size of all references of ``obj``.

        :param dict obj: Data object's output field (of type file/dir).
        :param Path obj_path: Path to ``obj``.
        """
        total_size = 0
        for ref in obj.get("refs", []):
            ref_path = data.location.get_path(filename=ref)
            if ref_path in os.fspath(obj_path):
                # It is a common case that ``obj['file']`` is also contained in
                # one of obj['ref']. In that case, we need to make sure that it's
                # size is not counted twice:
                continue
            ref_path: Path = Path(ref_path)
            if ref_path.is_file():
                total_size += ref_path.stat().st_size
            elif ref_path.is_dir():
                total_size += get_dir_size(ref_path)

        return total_size

    def add_file_size(obj):
        """Add file size to the basic:file field."""
        if (
            data.status in [Data.STATUS_DONE, Data.STATUS_ERROR]
            and "size" in obj
            and not force
        ):
            return

        path = Path(data.location.get_path(filename=obj["file"]))
        if not path.is_file():
            raise ValidationError("Referenced file does not exist ({})".format(path))

        obj["size"] = path.stat().st_size
        obj["total_size"] = obj["size"] + get_refs_size(obj, path)

    def add_dir_size(obj):
        """Add directory size to the basic:dir field."""
        if (
            data.status in [Data.STATUS_DONE, Data.STATUS_ERROR]
            and "size" in obj
            and not force
        ):
            return

        path = Path(data.location.get_path(filename=obj["dir"]))
        if not path.is_dir():
            raise ValidationError("Referenced dir does not exist ({})".format(path))

        obj["size"] = get_dir_size(path)
        obj["total_size"] = obj["size"] + get_refs_size(obj, path)

    data_size = 0
    for field_schema, fields in iterate_fields(data.output, data.process.output_schema):
        name = field_schema["name"]
        value = fields[name]
        if "type" in field_schema:
            if field_schema["type"].startswith("basic:file:"):
                add_file_size(value)
                data_size += value.get("total_size", 0)
            elif field_schema["type"].startswith("list:basic:file:"):
                for obj in value:
                    add_file_size(obj)
                    data_size += obj.get("total_size", 0)
            elif field_schema["type"].startswith("basic:dir:"):
                add_dir_size(value)
                data_size += value.get("total_size", 0)
            elif field_schema["type"].startswith("list:basic:dir:"):
                for obj in value:
                    add_dir_size(obj)
                    data_size += obj.get("total_size", 0)

    data.size = data_size
