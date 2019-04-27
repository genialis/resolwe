"""Process input or output fields."""
import collections
import json

import resolwe_runtime_utils


class ValidationError(Exception):
    """Field value validation error."""


class Field:
    """Process input or output field."""

    field_type = None

    def __init__(self, label=None, required=True, description=None, default=None, choices=None,
                 hidden=False):
        """Construct a field descriptor."""
        self.name = None
        self.process = None
        self.label = label
        self.required = required
        self.description = description
        self.default = default
        self.choices = choices
        self.hidden = hidden

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
            'name': self.name,
            'type': self.get_field_type(),
        }
        if self.required is not None:
            schema['required'] = self.required
        if self.label is not None:
            schema['label'] = self.label
        if self.description is not None:
            schema['description'] = self.description
        if self.default is not None:
            schema['default'] = self.default
        if self.hidden is not None:
            schema['hidden'] = self.hidden
        if self.choices is not None:
            for choice, label in self.choices:
                schema.setdefault('choices', []).append({
                    'label': label,
                    'value': choice,
                })

        return schema

    def to_output(self, value):
        """Convert value to process output format."""
        return json.loads(resolwe_runtime_utils.save(self.name, value))

    def validate(self, value):
        """Validate field value."""
        if self.required and value is None:
            raise ValidationError("field is required")

        if value is not None and self.choices is not None:
            choices = [choice for choice, _ in self.choices]
            if value not in choices:
                raise ValidationError("field must be one of: {}".format(
                    ", ".join(choices),
                ))

    def clean(self, value):
        """Run validators and return the clean value."""
        if value is None:
            value = self.default

        try:
            value = self.to_python(value)
            self.validate(value)
        except ValidationError as error:
            raise ValidationError("invalid value for {}: {}".format(
                self.name,
                error.args[0]
            ))
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

    field_type = 'basic:string'

    def validate(self, value):
        """Validate field value."""
        if value is not None and not isinstance(value, str):
            raise ValidationError("field must be a string")

        super().validate(value)


class TextField(StringField):
    """Text field."""

    field_type = 'basic:text'


class BooleanField(Field):
    """Boolean field."""

    field_type = 'basic:boolean'

    def validate(self, value):
        """Validate field value."""
        if value is not None and not isinstance(value, bool):
            raise ValidationError("field must be a boolean")

        super().validate(value)


class IntegerField(Field):
    """Integer field."""

    field_type = 'basic:integer'

    def to_python(self, value):
        """Convert value if needed."""
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                raise ValidationError("field must be an integer")

    def to_output(self, value):
        """Convert value to process output format."""
        return json.loads(resolwe_runtime_utils.save(self.name, str(value)))


class FloatField(Field):
    """Float field."""

    # TODO: Fix the underlying field into basic:float once that is renamed.
    field_type = 'basic:decimal'

    def to_python(self, value):
        """Convert value if needed."""
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                raise ValidationError("field must be a float")

    def to_output(self, value):
        """Convert value to process output format."""
        return json.loads(resolwe_runtime_utils.save(self.name, str(value)))


class DateField(Field):
    """Date field."""

    field_type = 'basic:date'


class DateTimeField(Field):
    """Date time field."""

    field_type = 'basic:datetime'


class UrlField(Field):
    """URL field."""

    # Url types.
    DOWNLOAD = 'download'
    VIEW = 'view'
    LINK = 'link'

    URL_TYPES = (DOWNLOAD, VIEW, LINK)

    def __init__(self, url_type, *args, **kwargs):
        """Construct an URL field descriptor.

        :param url_type: Type of URL
        """
        if url_type not in self.URL_TYPES:
            raise ValueError("url_type must be one of: {}".format(', '.join(self.URL_TYPES)))

        self.url_type = url_type
        super().__init__(*args, **kwargs)

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            try:
                value = value['url']
            except KeyError:
                raise ValidationError("dictionary must contain an 'url' element")

            if not isinstance(value, str):
                raise ValidationError("field's url element must be a string")

            return value
        elif not isinstance(value, None):
            raise ValidationError("field must be a string or a dict")

    def get_field_type(self):
        """Return this field's type."""
        return 'basic:url:{}'.format(self.url_type)


class SecretField(Field):
    """Secret field."""

    field_type = 'basic:secret'


class FileDescriptor:
    """Descriptor for accessing files."""

    def __init__(self, path, size=None, total_size=None, is_remote=False, file_temp=None, refs=None):
        """Construct a file descriptor."""
        self.path = path
        self.size = size
        self.total_size = total_size
        self.is_remote = is_remote
        self.file_temp = file_temp
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
        if not hasattr(resolwe_runtime_utils, 'import_file'):
            raise RuntimeError('Requires resolwe-runtime-utils >= 2.0.0')

        if imported_format is None:
            imported_format = resolwe_runtime_utils.ImportedFormat.BOTH

        return resolwe_runtime_utils.import_file(
            src=self.file_temp,
            file_name=self.path,
            imported_format=imported_format,
            progress_from=progress_from,
            progress_to=progress_to
        )

    def __repr__(self):
        """Return string representation."""
        return '<FileDescriptor path={}>'.format(self.path)


class FileField(Field):
    """File field."""

    field_type = 'basic:file'

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, FileDescriptor):
            return value
        elif isinstance(value, str):
            return FileDescriptor(value)
        elif isinstance(value, dict):
            try:
                path = value['file']
            except KeyError:
                raise ValidationError("dictionary must contain a 'file' element")

            if not isinstance(path, str):
                raise ValidationError("field's file element must be a string")

            size = value.get('size', None)
            if size is not None and not isinstance(size, int):
                raise ValidationError("field's size element must be an integer")

            total_size = value.get('total_size', None)
            if total_size is not None and not isinstance(total_size, int):
                raise ValidationError("field's total_size element must be an integer")

            is_remote = value.get('is_remote', None)
            if is_remote is not None and not isinstance(is_remote, bool):
                raise ValidationError("field's is_remote element must be a boolean")

            file_temp = value.get('file_temp', None)
            if file_temp is not None and not isinstance(file_temp, str):
                raise ValidationError("field's file_temp element must be a string")

            refs = value.get('refs', None)
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
            )
        elif not isinstance(value, None):
            raise ValidationError("field must be a FileDescriptor, string or a dict")

    def to_output(self, value):
        """Convert value to process output format."""
        return json.loads(resolwe_runtime_utils.save_file(self.name, value.path, *value.refs))


class FileHtmlField(FileField):
    """HTML file field."""

    field_type = 'basic:file:html'


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
        return '<DirDescriptor path={}>'.format(self.path)


class DirField(Field):
    """Directory field."""

    field_type = 'basic:dir'

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, DirDescriptor):
            return value
        elif isinstance(value, str):
            return DirDescriptor(value)
        elif isinstance(value, dict):
            try:
                path = value['dir']
            except KeyError:
                raise ValidationError("dictionary must contain a 'dir' element")

            if not isinstance(path, str):
                raise ValidationError("field's dir element must be a string")

            size = value.get('size', None)
            if size is not None and not isinstance(size, int):
                raise ValidationError("field's size element must be an integer")

            total_size = value.get('total_size', None)
            if total_size is not None and not isinstance(total_size, int):
                raise ValidationError("field's total_size element must be an integer")

            refs = value.get('refs', None)
            if refs is not None and not isinstance(refs, list):
                # TODO: Validate that all refs are strings.
                raise ValidationError("field's refs element must be a list of strings")

            return DirDescriptor(
                path,
                size=size,
                total_size=total_size,
                refs=refs,
            )
        elif not isinstance(value, None):
            raise ValidationError("field must be a DirDescriptor, string or a dict")

    def to_output(self, value):
        """Convert value to process output format."""
        return json.loads(resolwe_runtime_utils.save_dir(self.name, value.path, *value.refs))


class JsonField(Field):
    """JSON field."""

    field_type = 'basic:json'

    def to_python(self, value):
        """Convert value if needed."""
        if value is not None:
            return json.loads(json.dumps(value))


class ListField(Field):
    """Generic list field."""

    def __init__(self, inner, *args, **kwargs):
        """Construct a list field."""
        if not isinstance(inner, Field):
            raise TypeError("inner field must be an instance of Field")

        self.inner = inner
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
        return [self.inner.to_python(v) for v in value]

    def to_output(self, value):
        """Convert value to process output format."""
        return {self.name: [self.inner.to_output(v)[self.name] for v in value]}

    def get_field_type(self):
        """Return this field's type."""
        return 'list:{}'.format(self.inner.get_field_type())

    def validate(self, value):
        """Validate field value."""
        if value is not None:
            if not isinstance(value, list):
                raise ValidationError("field must be a list")

            for index, element in enumerate(value):
                try:
                    self.inner.validate(element)
                except ValidationError as error:
                    raise ValidationError("invalid element {}: {}".format(
                        index,
                        error.args[0],
                    ))

        super().validate(value)


class DataDescriptor:
    """Descriptor for accessing data objects."""

    def __init__(self, data_id, field, cache):
        """Construct a data descriptor.

        :param data_id: Data object primary key
        :param field: Field this descriptor is for
        :param cache: Optional cached object to use
        """
        super().__setattr__('_data_id', data_id)

        # Map output fields to a valid Python process syntax.
        for field_descriptor in cache['__output_schema']:
            field_name = field_descriptor['name']

            if field_name not in cache:
                # Non-required fields may be missing.
                cache[field_name] = None
                continue

            field_type = field_descriptor['type'].rstrip(':')

            if field_type.startswith('list:'):
                field = ListField(ALL_FIELDS_MAP[field_type[5:]]())
            else:
                field = ALL_FIELDS_MAP[field_type]()

            value = cache[field_name]
            value = field.clean(value)
            cache[field_name] = value

        super().__setattr__('_cache', cache)

    def _populate_cache(self):
        """Fetch data object from the backend if needed."""
        if self._data_id is None:
            return

        if self._cache is None:
            # TODO: Implement fetching via the protocol once available.
            raise NotImplementedError

    def _get(self, key):
        """Return given key from cache."""
        self._populate_cache()
        if key not in self._cache:
            raise AttributeError("DataField has no member {}".format(key))

        return self._cache[key]

    @property
    def id(self):  # pylint: disable=invalid-name
        """Primary key of this data object."""
        return self._data_id

    @property
    def type(self):
        """Type of this data object."""
        return self._get('__type')

    @property
    def descriptor(self):
        """Descriptor of this data object."""
        return self._get('__descriptor')

    @property
    def entity_name(self):
        """Entity name."""
        return self._get('__entity_name')

    def __getattr__(self, key):
        """Get attribute."""
        return self._get(key)

    def __setattr__(self, key, value):
        """Set attribute."""
        raise AttributeError("inputs are read-only")

    def __repr__(self):
        """Return string representation."""
        return '<DataDescriptor id={}>'.format(self._data_id)


class DataField(Field):
    """Data object field."""

    def __init__(self, data_type, *args, **kwargs):
        """Construct a data field."""
        # TODO: Validate data type format.
        self.data_type = data_type
        super().__init__(*args, **kwargs)

    def get_field_type(self):
        """Return this field's type."""
        return 'data:{}'.format(self.data_type)

    def to_python(self, value):
        """Convert value if needed."""
        cache = None
        if value is None:
            return None

        if isinstance(value, DataDescriptor):
            return value
        elif isinstance(value, dict):
            # Allow pre-hydrated data objects.
            cache = value
            try:
                value = cache['__id']
            except KeyError:
                raise ValidationError("dictionary must contain an '__id' element")
        elif not isinstance(value, int):
            raise ValidationError("field must be a DataDescriptor, data object primary key or dict")

        return DataDescriptor(value, self, cache)


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

    field_type = 'basic:group'

    def __init__(self, field_group, label=None, description=None, disabled=False, collapsed=False,
                 hidden=False):
        """Construct a group field."""
        super().__init__(
            label=label,
            required=None,
            description=description,
            hidden=hidden,
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
            if field_name.startswith('_'):
                continue

            field = getattr(self.field_group, field_name)
            field.contribute_to_class(process, self.fields, field_name)

        super().contribute_to_class(process, fields, name)

    def to_python(self, value):
        """Convert value if needed."""
        if isinstance(value, GroupDescriptor):
            value = value._value  # pylint: disable=protected-access

        result = {}
        for name, field in self.fields.items():
            result[name] = field.to_python(value.get(name, None))

        return GroupDescriptor(result)

    def to_schema(self):
        """Return field schema for this field."""
        schema = super().to_schema()
        if self.disabled is not None:
            schema['disabled'] = self.disabled
        if self.collapsed is not None:
            schema['collapsed'] = self.collapsed

        group = []
        for field in self.fields.values():
            group.append(field.to_schema())
        schema['group'] = group

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

ALL_FIELDS_MAP = {field.field_type: field for field in ALL_FIELDS}


def get_available_fields():
    """Return a list of available field classes."""
    return ALL_FIELDS
