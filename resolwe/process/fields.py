"""Process input or output fields."""
import collections
import glob
import gzip
import os
import re
import shlex
import shutil
import subprocess
import tarfile
import zlib
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Type, Union

import requests

from .communicator import communicator

DATA_LOCAL_VOLUME = Path(os.environ.get("DATA_LOCAL_VOLUME", "/data_local"))
DATA_VOLUME = Path(os.environ.get("DATA_VOLUME", "/data"))
DATA_ALL_VOLUME = Path(os.environ.get("DATA_ALL_VOLUME", "/data_all"))


def _get_dir_size(path):
    """Get directory size.

    :param path: a Path object pointing to the directory.
    :type path: pathlib.Path
    """
    return sum(
        file_.stat().st_size for file_ in Path(path).rglob("*") if file_.is_file()
    )


def _copy_file_or_dir(entries: Iterable[Union[str, Path]]):
    """Copy file and all its references to data_volume.

    The entry is a path relative to the DATA_LOCAL_VOLUME (our working
    directory). It must be copied to the DATA_VOLUME on the shared
    filesystem.
    """
    for entry in entries:
        source = Path(entry)
        destination = DATA_VOLUME / source
        destination.parent.mkdir(exist_ok=True, parents=True)
        if source.is_dir():
            if not destination.exists():
                shutil.copytree(source, destination)
            else:
                # If destination directory exists the copytree will fail.
                # In such case perform a recursive call with entries in
                # the source directory as arguments.
                #
                # TODO: fix when we support Python 3.8 and later. See
                # dirs_exist_ok argument to copytree method.
                _copy_file_or_dir(source.glob("*"))
        elif source.is_file():
            destination.parent.mkdir(exist_ok=True, parents=True)
            # Use copy2 to preserve file metadata, such as file creation
            # and modification times.
            shutil.copy2(source, destination)


class ValidationError(Exception):
    """Field value validation error."""


# ------Import file attributes ----------.
class ImportedFormat:
    """Import destination file format."""

    EXTRACTED = "extracted"
    COMPRESSED = "compressed"
    BOTH = "both"


# ----------------------------------


class Field:
    """Process input or output field."""

    field_type = None

    def __init__(
        self,
        label=None,
        required=True,
        description=None,
        default=None,
        choices=None,
        allow_custom_choice=None,
        hidden=False,
        *args,
        **kwargs,
    ):
        """Construct a field descriptor."""
        self.name = None
        self.process: Optional["resolwe.process.descriptor.ProcessDescriptor"] = None
        self.label = label
        self.required = required
        self.description = description
        self.default = default
        self.choices = choices
        self.allow_custom_choice = allow_custom_choice
        self.hidden = hidden

    @property
    def _descriptor_field_name(self):
        """Get descriptor field name."""
        return f"{self.name}"

    def __get__(self, obj, objtype=None):
        """Make field a descriptor."""
        if communicator is None or obj is None:
            return self
        field_name = self._descriptor_field_name
        private_name = f"_{field_name}"
        if obj._model_name == "Data" and obj._pk == 1:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(private_name)

        if not hasattr(obj, private_name):
            if obj._model_name == "Data" and obj._pk == 1:
                print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            result = communicator.get_model_fields(
                obj._model_name, obj._pk, [field_name]
            )
            print(f"Got result for {self.name}: ", result)
            if len(result) > 1:
                result = [e[field_name] for e in result]
            else:
                result = result[field_name]
            value = self.clean(result)
            setattr(obj, private_name, value)
        value = getattr(obj, private_name)
        print(f"Object: {obj}, type: {objtype}")
        print(f"Getting value of the field: {self.name}: {value}")
        return value

    def __set__(self, obj, value):
        """Make field a descriptor."""
        if communicator is None:
            return
        field_name = self._descriptor_field_name
        private_name = f"_{field_name}"
        setattr(self, private_name, value)
        output = self.to_output(value)
        communicator.update_model_fields(obj._model_name, obj._pk, {field_name: output})

    def get_field_type(self):
        """Return this field's type."""
        return self.field_type

    def contribute_to_class(self, process, fields, name):
        """Register this field with a specific process.

        :param process: Process descriptor instance
        :param fields: Fields registry to use
        :param name: Field name
        """
        self.name = name
        self.process = process
        fields[name] = self

    def to_python(self, value):
        """Convert value if needed."""
        return value

    def to_schema(self):
        """Return field schema for this field."""
        if not self.name or not self.process:
            raise ValueError("field is not registered with process")

        schema = {
            "name": self.name,
            "type": self.get_field_type(),
        }
        if self.required is not None:
            schema["required"] = self.required
        if self.label is not None:
            schema["label"] = self.label
        if self.description is not None:
            schema["description"] = self.description
        if self.default is not None:
            schema["default"] = self.default
        if self.hidden is not None:
            schema["hidden"] = self.hidden
        if self.allow_custom_choice is not None:
            schema["allow_custom_choice"] = self.allow_custom_choice
        if self.choices is not None:
            for choice, label in self.choices:
                schema.setdefault("choices", []).append(
                    {
                        "label": label,
                        "value": choice,
                    }
                )

        return schema

    def to_list_schema(self, *args, **kwargs):
        """Return part of list field schema that is particular to this field."""
        return {}

    def to_output(self, value):
        """Convert value to process output format.

        :returns: dict {name, value}.
        """
        return value

    def validate(self, value):
        """Validate field value."""
        if self.required and value is None:
            raise ValidationError("field is required")

        if value is not None and self.choices is not None:
            choices = [choice for choice, _ in self.choices]
            if value not in choices and not self.allow_custom_choice:
                raise ValidationError(
                    "field must be one of: {}".format(", ".join(choices))
                )

    def clean(self, value):
        """Run validators and return the clean value."""
        if value is None:
            value = self.default

        try:
            value = self.to_python(value)
            self.validate(value)
        except ValidationError as error:
            raise ValidationError(
                "invalid value for {}: {}".format(self.name, error.args[0])
            )
        return value

    def __repr__(self):
        """Return string representation."""
        return '<{klass} name={name} type={type} label="{label}">'.format(
            klass=self.__class__.__name__,
            name=self.name,
            type=self.get_field_type(),
            label=self.label,
        )


class StringField(Field):
    """String field."""

    field_type = "basic:string"

    def validate(self, value):
        """Validate field value."""
        if value is not None and not isinstance(value, str):
            raise ValidationError("field must be a string")

        super().validate(value)


class TextField(StringField):
    """Text field."""

    field_type = "basic:text"


class BooleanField(Field):
    """Boolean field."""

    field_type = "basic:boolean"

    def validate(self, value):
        """Validate field value."""
        if value is not None and not isinstance(value, bool):
            raise ValidationError("field must be a boolean")

        super().validate(value)


class IntegerField(Field):
    """Integer field."""

    field_type = "basic:integer"

    def to_python(self, value):
        """Convert value if needed."""
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                raise ValidationError("field must be an integer")


class FloatField(Field):
    """Float field."""

    # TODO: Fix the underlying field into basic:float once that is renamed.
    field_type = "basic:decimal"

    def to_python(self, value):
        """Convert value if needed."""
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValidationError("field must be a float")


class DateField(Field):
    """Date field."""

    field_type = "basic:date"


class DateTimeField(Field):
    """Date time field."""

    field_type = "basic:datetime"


class UrlField(Field):
    """URL field."""

    # Url types.
    DOWNLOAD = "download"
    VIEW = "view"
    LINK = "link"

    URL_TYPES = (DOWNLOAD, VIEW, LINK)

    def __init__(self, url_type, *args, **kwargs):
        """Construct an URL field descriptor.

        :param url_type: Type of URL
        """
        if url_type not in self.URL_TYPES:
            raise ValueError(
                "url_type must be one of: {}".format(", ".join(self.URL_TYPES))
            )

        self.url_type = url_type
        super().__init__(*args, **kwargs)

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            try:
                value = value["url"]
            except KeyError:
                raise ValidationError("dictionary must contain an 'url' element")

            if not isinstance(value, str):
                raise ValidationError("field's url element must be a string")

            return value
        elif not isinstance(value, None):
            raise ValidationError("field must be a string or a dict")

    def get_field_type(self):
        """Return this field's type."""
        return "basic:url:{}".format(self.url_type)


class DownloadUrlField(UrlField):
    """Subclass of UrlField."""

    field_type = "basic:url:download"

    def __init__(self, *args, **kwargs):
        """Init."""
        super().__init__(UrlField.DOWNLOAD, *args, **kwargs)


class ViewUrlField(UrlField):
    """Subclass of UrlField."""

    field_type = "basic:url:view"

    def __init__(self, *args, **kwargs):
        """Init."""
        super().__init__(UrlField.VIEW, *args, **kwargs)


class LinkUrlField(UrlField):
    """Subclass of UrlField."""

    field_type = "basic:url:link"

    def __init__(self, *args, **kwargs):
        """Init."""
        super().__init__(UrlField.LINK, *args, **kwargs)


class SecretField(Field):
    """Secret field."""

    field_type = "basic:secret"


class FileDescriptor:
    """Descriptor for accessing files."""

    CHUNK_SIZE = 10_000_000  # 10 Mbytes

    def __init__(
        self,
        path,
        size=None,
        total_size=None,
        is_remote=False,
        file_temp=None,
        refs=None,
        file_field: Optional["FileField"] = None,
    ):
        """Construct a file descriptor."""
        self.path = path
        self.size = size
        self.total_size = total_size
        self.is_remote = is_remote
        self.file_temp = file_temp
        self.file_field = file_field
        if refs is None:
            refs = []
        self.refs = refs

    def import_file(self, imported_format=None, progress_from=0.0, progress_to=None):
        """Import field source file to working directory.

        :param imported_format: Import file format (extracted, compressed or both)
        :param progress_from: Initial progress value
        :param progress_to: Final progress value
        :return: Destination file path (if extracted and compressed, extracted path given)
        """
        if imported_format is None:
            imported_format = ImportedFormat.BOTH

        src = self.file_temp
        file_name = self.path

        if progress_to is not None:
            if not isinstance(progress_from, float) or not isinstance(
                progress_to, float
            ):
                raise ValueError("Progress_from and progress_to must be float")

            if progress_from < 0 or progress_from > 1:
                raise ValueError("Progress_from must be between 0 and 1")

            if progress_to < 0 or progress_to > 1:
                raise ValueError("Progress_to must be between 0 and 1")

            if progress_from >= progress_to:
                raise ValueError("Progress_to must be higher than progress_from")

        print("Importing and compressing {}...".format(file_name))

        def importGz():
            """Import gzipped file.

            The file_name must have .gz extension.
            """
            if imported_format != ImportedFormat.COMPRESSED:  # Extracted file required
                with open(file_name[:-3], "wb") as f_out, gzip.open(src, "rb") as f_in:
                    try:
                        shutil.copyfileobj(f_in, f_out, FileDescriptor.CHUNK_SIZE)
                    except zlib.error:
                        raise ValueError(
                            "Invalid gzip file format: {}".format(file_name)
                        )

            else:  # Extracted file not-required
                # Verify the compressed file.
                with gzip.open(src, "rb") as f:
                    try:
                        while f.read(FileDescriptor.CHUNK_SIZE) != b"":
                            pass
                    except zlib.error:
                        raise ValueError(
                            "Invalid gzip file format: {}".format(file_name)
                        )

            if imported_format != ImportedFormat.EXTRACTED:  # Compressed file required
                try:
                    shutil.copyfile(src, file_name)
                except shutil.SameFileError:
                    pass  # Skip copy of downloaded files

            if imported_format == ImportedFormat.COMPRESSED:
                return file_name
            else:
                return file_name[:-3]

        def import7z():
            """Import compressed file in various formats.

            Supported extensions: .bz2, .zip, .rar, .7z, .tar.gz, and .tar.bz2.
            """
            extracted_name, _ = os.path.splitext(file_name)
            destination_name = extracted_name
            temp_dir = "temp_{}".format(extracted_name)

            # TODO: is this a problem? The 7z binary must be present.
            cmd = "7z x -y -o{} {}".format(shlex.quote(temp_dir), shlex.quote(src))
            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError as err:
                if err.returncode == 2:
                    raise ValueError("Failed to extract file: {}".format(file_name))
                else:
                    raise

            paths = os.listdir(temp_dir)
            if len(paths) == 1 and os.path.isfile(os.path.join(temp_dir, paths[0])):
                # Single file in archive.
                temp_file = os.path.join(temp_dir, paths[0])

                if (
                    imported_format != ImportedFormat.EXTRACTED
                ):  # Compressed file required
                    with open(temp_file, "rb") as f_in, gzip.open(
                        extracted_name + ".gz", "wb"
                    ) as f_out:
                        shutil.copyfileobj(f_in, f_out, FileDescriptor.CHUNK_SIZE)

                if (
                    imported_format != ImportedFormat.COMPRESSED
                ):  # Extracted file required
                    shutil.move(temp_file, "./{}".format(extracted_name))

                    if extracted_name.endswith(".tar"):
                        with tarfile.open(extracted_name) as tar:
                            tar.extractall()

                        os.remove(extracted_name)
                        destination_name, _ = os.path.splitext(extracted_name)
                else:
                    destination_name = extracted_name + ".gz"
            else:
                # Directory or several files in archive.
                if (
                    imported_format != ImportedFormat.EXTRACTED
                ):  # Compressed file required
                    with tarfile.open(extracted_name + ".tar.gz", "w:gz") as tar:
                        for fname in glob.glob(os.path.join(temp_dir, "*")):
                            tar.add(fname, os.path.basename(fname))

                if (
                    imported_format != ImportedFormat.COMPRESSED
                ):  # Extracted file required
                    for path in os.listdir(temp_dir):
                        shutil.move(os.path.join(temp_dir, path), "./{}".format(path))
                else:
                    destination_name = extracted_name + ".tar.gz"

            shutil.rmtree(temp_dir)
            return destination_name

        def importUncompressed():
            """Import uncompressed file."""
            if imported_format != ImportedFormat.EXTRACTED:  # Compressed file required
                with open(src, "rb") as f_in, gzip.open(
                    file_name + ".gz", "wb"
                ) as f_out:
                    shutil.copyfileobj(f_in, f_out, FileDescriptor.CHUNK_SIZE)

            if imported_format != ImportedFormat.COMPRESSED:  # Extracted file required
                try:
                    print(f"Got {file_name}.")
                    print(f"CWD: {os.getcwd()}.")
                    shutil.copyfile(src, file_name)
                except shutil.SameFileError:
                    pass  # Skip copy of downloaded files

            return (
                file_name + ".gz"
                if imported_format == ImportedFormat.COMPRESSED
                else file_name
            )

        # Large file download from Google Drive requires cookie and token.
        try:
            response = None
            if re.match(
                r"^https://drive.google.com/[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]$",
                src,
            ):
                session = requests.Session()
                response = session.get(src, stream=True)

                token = None
                for key, value in response.cookies.items():
                    if key.startswith("download_warning"):
                        token = value
                        break

                if token is not None:
                    params = {"confirm": token}
                    response = session.get(src, params=params, stream=True)

            elif re.match(
                r"^(https?|ftp)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]$",
                src,
            ):
                response = requests.get(src, stream=True)
        except requests.exceptions.ConnectionError:
            raise requests.exceptions.ConnectionError(
                "Could not connect to {}".format(src)
            )

        if response:
            with open(file_name, "wb") as f:
                total = response.headers.get("content-length")
                total = float(total) if total else None
                downloaded = 0
                current_progress = 0
                for content in response.iter_content(
                    chunk_size=FileDescriptor.CHUNK_SIZE
                ):
                    f.write(content)

                    if total is not None and progress_to is not None:
                        downloaded += len(content)
                        progress_span = progress_to - progress_from
                        next_progress = (
                            progress_from + progress_span * downloaded / total
                        )
                        next_progress = round(next_progress, 2)

                        if next_progress > current_progress:
                            if (
                                self.file_field is not None
                                and self.file_field.process is not None
                            ):
                                print(f"Reporting progress: {next_progress}")
                                communicator.progress(next_progress)
                            current_progress = next_progress

            # Check if a temporary file exists.
            if not os.path.isfile(file_name):
                raise ValueError("Downloaded file not found {}".format(file_name))

            src = file_name
        else:
            # If scr is file it needs to have upload directory prepended.
            upload_dir = Path(os.environ.get("UPLOAD_DIR", "/upload"))
            src_path = upload_dir / src
            if not src_path.is_file():
                raise ValueError(f"Source file not found {src}")
            src = os.fspath(src_path)

        # Decide which import should be used.
        if re.search(r"\.(bz2|zip|rar|7z|tgz|tar\.gz|tar\.bz2)$", file_name):
            destination_file_name = import7z()
        elif file_name.endswith(".gz"):
            destination_file_name = importGz()
        else:
            destination_file_name = importUncompressed()

        if (
            progress_to is not None
            and self.file_field is not None
            and self.file_field.process is not None
        ):
            print(f"Reporting finall progress: {progress_to}")
            print(self.file_field)
            print(self.file_field.process)
            communicator.progress(progress_to)

        return destination_file_name

    def __repr__(self):
        """Return string representation."""
        return "<FileDescriptor path={}>".format(self.path)


class FileField(Field):
    """File field."""

    field_type = "basic:file"

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, FileDescriptor):
            return value
        elif isinstance(value, str):
            return FileDescriptor(value, file_field=self)
        elif isinstance(value, dict):
            try:
                # TODO: here we have to hydrate, get the whole path.
                # TODO: make in nicer than hardcoded.
                path = value["file"]
            except KeyError:
                raise ValidationError("dictionary must contain a 'file' element")

            if not isinstance(path, str):
                raise ValidationError("field's file element must be a string")

            size = value.get("size", None)
            if size is not None and not isinstance(size, int):
                raise ValidationError("field's size element must be an integer")

            total_size = value.get("total_size", None)
            if total_size is not None and not isinstance(total_size, int):
                raise ValidationError("field's total_size element must be an integer")

            is_remote = value.get("is_remote", None)
            if is_remote is not None and not isinstance(is_remote, bool):
                raise ValidationError("field's is_remote element must be a boolean")

            file_temp = value.get("file_temp", None)
            if file_temp is not None and not isinstance(file_temp, str):
                raise ValidationError("field's file_temp element must be a string")

            refs = value.get("refs", None)
            if refs is not None and not isinstance(refs, list):
                # TODO: Validate that all refs are strings.
                raise ValidationError("field's refs element must be a list of strings")

            return FileDescriptor(
                path,
                size=size,
                total_size=total_size,
                is_remote=is_remote,
                file_temp=file_temp,
                refs=refs,
                file_field=self,
            )
        elif not isinstance(value, None):
            raise ValidationError("field must be a FileDescriptor, string or a dict")

    def to_output(self, value):
        """Convert value to process output format.

        Also copy the referenced file to the data volume.
        """
        data = {"file": value.path, "size": Path(value.path).stat().st_size}
        if value.refs:
            missing_refs = [
                ref
                for ref in value.refs
                if not (Path(ref).is_file() or Path(ref).is_dir())
            ]
            if missing_refs:
                raise Exception(
                    "Output '{}' set to missing references: '{}'.".format(
                        self.name, ", ".join(missing_refs)
                    )
                )
            data["refs"] = value.refs
        _copy_file_or_dir(chain(data.get("refs", []), (data["file"],)))
        return data


class FileHtmlField(FileField):
    """HTML file field."""

    field_type = "basic:file:html"


class DirDescriptor:
    """Descriptor for accessing directories."""

    def __init__(self, path, size=None, total_size=None, refs=None):
        """Construct a file descriptor."""
        self.path = path
        self.size = size
        self.total_size = total_size
        if refs is None:
            refs = []
        self.refs = refs

    def __repr__(self):
        """Return string representation."""
        return "<DirDescriptor path={}>".format(self.path)


class DirField(Field):
    """Directory field."""

    field_type = "basic:dir"

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, DirDescriptor):
            return value
        elif isinstance(value, str):
            return DirDescriptor(value)
        elif isinstance(value, dict):
            try:
                path = value["dir"]
            except KeyError:
                raise ValidationError("dictionary must contain a 'dir' element")

            if not isinstance(path, str):
                raise ValidationError("field's dir element must be a string")

            size = value.get("size", None)
            if size is not None and not isinstance(size, int):
                raise ValidationError("field's size element must be an integer")

            total_size = value.get("total_size", None)
            if total_size is not None and not isinstance(total_size, int):
                raise ValidationError("field's total_size element must be an integer")

            refs = value.get("refs", None)
            if refs is not None and not isinstance(refs, list):
                # TODO: Validate that all refs are strings.
                raise ValidationError("field's refs element must be a list of strings")

            return DirDescriptor(path, size=size, total_size=total_size, refs=refs)
        elif not isinstance(value, None):
            raise ValidationError("field must be a DirDescriptor, string or a dict")

    def to_output(self, value):
        """Convert value to process output format."""
        data = {"dir": value.path, "size": _get_dir_size(Path(value.path))}
        if value.refs:
            missing_refs = [
                ref
                for ref in value.refs
                if not (Path(ref).is_file() or Path(ref).is_dir())
            ]
            if missing_refs:
                raise Exception(
                    "Output '{}' set to missing references: '{}'.".format(
                        self.name, ", ".join(missing_refs)
                    )
                )
            data["refs"] = value.refs

        _copy_file_or_dir(chain(data.get("refs", []), (data["dir"],)))
        return data


class JsonField(Field):
    """JSON field."""

    field_type = "basic:json"

    def __init__(self, *args, **kwargs):
        """JSON field init."""
        self._model_instance = None
        super().__init__(*args, **kwargs)

    def __get__(self, obj, objtype=None):
        """Override parent method."""
        self._model_instance = obj
        print(f"Descriptor {self.name} get")
        print(f"Model: {obj}, type: {objtype}")
        return super().__get__(obj, objtype)

    def to_python(self, value):
        """Convert value if needed."""
        from .models import JSONDescriptor

        print(f"JSONField {self.name} to_python: {value}, {type(value)}")

        if isinstance(value, JSONDescriptor):
            return value
        assert self._model_instance is not None

        if isinstance(value, dict):
            schema = None
            if self._model_instance._model_name == "Data":
                if self.name in [
                    "input",
                    "output",
                ]:
                    schema_name = f"{self.name}_schema"
                    print(f"Getting schema {schema_name}")
                    schema = getattr(self._model_instance.process, schema_name)
                    print(f"Got schema for {self.name}: {schema}")
                if self.name == "descriptor":
                    print("Get descriptor schema")
                    model_schema = self._model_instance.descriptor_schema
                    print(f"Got descriptor schema: {schema}")
                    if model_schema is not None:
                        schema = model_schema.schema

            return JSONDescriptor(
                self._model_instance,
                self.name,
                cache=value,
                field_schema=schema,
            )
        return value

    def to_output(self, value):
        """Convect to output format."""
        raise RuntimeError("Only fields of JSON property can be set.")


class ListField(Field):
    """Generic list field."""

    def __init__(self, inner, *args, **kwargs):
        """Construct a list field."""
        if not isinstance(inner, Field):
            raise TypeError("inner field must be an instance of Field")

        self.inner = inner
        self.args = args
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)

    def contribute_to_class(self, process, fields, name):
        """Register this field with a specific process.

        :param process: Process descriptor instance
        :param fields: Fields registry to use
        :param name: Field name
        """
        super().contribute_to_class(process, fields, name)

        self.inner.name = name
        self.inner.process = process

    def to_python(self, value):
        """Convert value if needed."""
        # A hack for ManyToMany with one relation.
        if isinstance(value, int):
            value = [value]
        return [self.inner.to_python(v) for v in value]

    def to_schema(self):
        """Return field schema for this field."""
        schema = super().to_schema()
        schema.update(self.inner.to_list_schema(*self.args, **self.kwargs))
        return schema

    def to_output(self, value):
        """Convert value to process output format."""
        return [self.inner.to_output(v) for v in value]

    def get_field_type(self):
        """Return this field's type."""
        return "list:{}".format(self.inner.get_field_type())

    def validate(self, value):
        """Validate field value."""
        if value is not None:
            if not isinstance(value, list):
                raise ValidationError("field must be a list")

            for index, element in enumerate(value):
                try:
                    self.inner.validate(element)
                except ValidationError as error:
                    raise ValidationError(
                        "invalid element {}: {}".format(index, error.args[0])
                    )

        super().validate(value)


class RelationPartitionDescriptor:
    """Descriptor for accessing relation partitions."""

    def __init__(self, entity_id, position=None, label=None):
        """Construct a relation partition descriptor."""
        self.entity_id = entity_id
        self.position = position
        self.label = label


class RelationDescriptor:
    """Descriptor for accessing relations between data / entities."""

    def __init__(self, id, type, ordered, category, partitions, unit=None):
        """Construct a relation descriptor."""
        self.id = id
        self.type = type
        self.ordered = ordered
        self.category = category
        self.unit = unit
        self.partitions = partitions

    def __eq__(self, other):
        """Compare equality."""
        if isinstance(other, RelationDescriptor):
            return self.id == other.id
        return False

    def __hash__(self):
        """Get hash value."""
        return hash(self.id)

    @classmethod
    def from_dict(cls, data):
        """Create relation descriptor from a dictionary."""
        id = data["relation_id"]
        type = data["relation_type_name"]
        ordered = data["relation_type_ordered"]
        category = data["category"]
        unit = data.get("unit", None)

        partitions = []
        for partitinon_data in data["partitions"]:
            partition = RelationPartitionDescriptor(
                entity_id=partitinon_data["entity_id"],
                position=partitinon_data.get("position"),
                label=partitinon_data.get("label"),
            )
            partitions.append(partition)

        return cls(
            id=id,
            type=type,
            ordered=ordered,
            category=category,
            partitions=partitions,
            unit=unit,
        )


def fields_from_schema(schema: List[dict]) -> Dict[str, Field]:
    """Get fields from schema (input or output)."""
    fields: Dict[str, Field] = dict()
    field: Optional[Field] = None
    for field_descriptor in schema:
        field_name = field_descriptor["name"]
        field_type = field_descriptor["type"].rstrip(":")

        if field_type.startswith("list:"):
            if field_type.startswith("list:data"):
                field_class = DataField
            else:
                field_class = ALL_FIELDS_MAP[field_type[len("list:") :]]
            extra_kwargs: dict = field_descriptor
            if issubclass(field_class, DataField):
                extra_kwargs["data_type"] = field_type[len("list:data:") :]
            field = ListField(field_class(**extra_kwargs))
        else:
            if field_type.startswith("data:"):
                field_class = DataField
            else:
                field_class = ALL_FIELDS_MAP[field_type]
            extra_kwargs = {}
            if issubclass(field_class, DataField):
                extra_kwargs["data_type"] = field_type[len("data:") :]
            if issubclass(field_class, GroupField):
                group_schema = field_descriptor["group"]
                field_group = fields_from_schema(group_schema)

                class FieldGroup:
                    def __init__(self, values):
                        self.__dict__.update(values)

                fg = FieldGroup(field_group)
                extra_kwargs["field_group"] = fg
            field = field_class(**extra_kwargs)
        fields[field_name] = field
        field.name = field_name
    return fields


class DataField(Field):
    """Data object field."""

    field_type = "data"

    def __init__(
        self, data_type, relation_type=None, relation_npartitions=None, *args, **kwargs
    ):
        """Construct a data field."""
        # TODO: Validate data type format.
        self.data_type = data_type
        self.relation_type = relation_type
        self.relation_npartitions = relation_npartitions
        super().__init__(*args, **kwargs)

    def get_field_type(self):
        """Return this field's type."""
        return "data:{}".format(self.data_type)

    @staticmethod
    def _generate_relation(relation_type, relation_npartitions):
        """Generate relation part of data field schema."""
        if relation_npartitions is not None and relation_type is None:
            raise AttributeError(
                "relation_type should be set when relation_npartition is not None."
            )

        if relation_type is None and relation_npartitions is None:
            return {}

        return {
            "relation": {
                "type": relation_type,
                "npartitions": relation_npartitions or "none",
            }
        }

    def to_schema(self):
        """Return field schema for this field."""
        schema = super().to_schema()

        relation = self._generate_relation(
            self.relation_type, self.relation_npartitions
        )
        schema.update(relation)

        return schema

    def to_list_schema(
        self, relation_type=None, relation_npartitions=None, *args, **kwargs
    ):
        """Add relation informations to list data field."""
        return self._generate_relation(relation_type, relation_npartitions)

    def to_python(self, value):
        """Convert value if needed."""
        from .models import Data

        if value is None:
            return None

        if isinstance(value, Data):
            return value

        elif isinstance(value, int):
            return Data(value)

        else:
            raise ValidationError("field must be a DataDescriptor or int")


class GroupDescriptor:
    """Group descriptor."""

    def __init__(self, value):
        """Construct a group descriptor."""
        self._value = value

    def __getattr__(self, name):
        """Get attribute."""
        try:
            return self._value[name]
        except KeyError:
            raise AttributeError(name)


class GroupField(Field):
    """Group field."""

    field_type = "basic:group"

    def __init__(
        self,
        field_group,
        label=None,
        description=None,
        disabled=False,
        collapsed=False,
        hidden=False,
    ):
        """Construct a group field."""
        super().__init__(
            label=label, required=None, description=description, hidden=hidden
        )

        self.disabled = disabled
        self.collapsed = collapsed
        self.field_group = field_group
        self.fields = collections.OrderedDict()

    def contribute_to_class(self, process, fields, name):
        """Register this field with a specific process.

        :param process: Process descriptor instance
        :param fields: Fields registry to use
        :param name: Field name
        """
        # Use order-preserving definition namespace (__dict__) to respect the
        # order of GroupField's fields definition.
        for field_name in self.field_group.__dict__:
            if field_name.startswith("_"):
                continue

            field = getattr(self.field_group, field_name)
            field.contribute_to_class(process, self.fields, field_name)

        super().contribute_to_class(process, fields, name)

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, GroupDescriptor):
            value = value._value

        result = {}
        for name, field in self.fields.items():
            result[name] = field.to_python(value.get(name, None))

        return GroupDescriptor(result)

    def to_schema(self):
        """Return field schema for this field."""
        schema = super().to_schema()
        if self.disabled is not None:
            schema["disabled"] = self.disabled
        if self.collapsed is not None:
            schema["collapsed"] = self.collapsed

        group = []
        for field in self.fields.values():
            group.append(field.to_schema())
        schema["group"] = group

        return schema


# List of available fields.
ALL_FIELDS = [
    StringField,
    TextField,
    BooleanField,
    IntegerField,
    FloatField,
    DateField,
    DateTimeField,
    DownloadUrlField,
    ViewUrlField,
    LinkUrlField,
    UrlField,
    SecretField,
    FileField,
    FileHtmlField,
    DirField,
    JsonField,
    ListField,
    DataField,
    GroupField,
]

ALL_FIELDS_MAP: Dict[str, Type[Field]] = {
    field.field_type: field for field in ALL_FIELDS
}


def get_available_fields():
    """Return a list of available field classes."""
    return ALL_FIELDS
