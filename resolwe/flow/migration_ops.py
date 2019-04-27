"""Resolwe process schema migration operations."""
import inspect

import jsonschema

from django.db.migrations.operations import base

from resolwe.flow.models.utils import validate_process_types, validation_schema
from resolwe.flow.utils import dict_dot

FIELD_SCHEMA = validation_schema('field')


class DataDefaultOperation:
    """Abstract data default generator."""

    def prepare(self, data, from_state):
        """Prepare data object migration.

        Called after all relevant process schemas have already
        been migrated.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """
        raise NotImplementedError

    def get_default_for(self, data, from_state):
        """Return default for given data object.

        :param data: Data object instance
        :param from_state: Database model state
        """
        raise NotImplementedError


class ConstantDataDefault(DataDefaultOperation):
    """Data default, which generates constant values."""

    def __init__(self, value):
        """Initialize constant data default.

        :param value: Constant default value to be used for all
            Data objects
        """
        self.value = value

    def prepare(self, data, from_state):
        """Prepare data object migration.

        Called after all relevant process schemas have already
        been migrated.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """
        return

    def get_default_for(self, data, from_state):
        """Return default for given data object.

        :param data: Data object instance
        :param from_state: Database model state
        """
        return self.value


class ResolweProcessOperation(base.Operation):
    """Abstract resolwe process migration operation."""

    reduces_to_sql = False
    atomic = True
    reversible = False

    def __init__(self, process, schema_type, **kwargs):
        """
        Construct a process migration operation.

        :param process: Process slug
        :param schema_type: Type of schema (must be either ``input`` or
            ``output``)
        """
        self.process = process
        self.schema_type = schema_type
        if self.schema_type not in ('input', 'output'):
            raise ValueError("Schema type must be either input or output")

        # Unfortunately, we can't easily get the identifier of the current
        # migration, which we need so we can update migration history. Here,
        # we assume that the Python implementation supports inspecting the
        # stack and we extract the parent migration's __module__ from there.
        migration_id = None
        current_frame = inspect.currentframe().f_back
        while current_frame:
            info = inspect.getframeinfo(current_frame)
            if info.function != 'Migration':
                current_frame = current_frame.f_back
                continue

            migration_id = current_frame.f_locals['__module__']
            break

        if not migration_id:
            raise ValueError("Unable to determine parent migration")

        self.migration_id = migration_id

    def deconstruct(self):
        """Deconstruct operation."""
        return (
            self.__class__.__name__,
            [],
            {
                'process': self.process,
                'schema_type': self.schema_type,
            }
        )

    def state_forwards(self, app_label, state):
        """Operation does not alter database state, only data."""

    def migrate_process_schema(self, process, schema, from_state):
        """Migrate process schema.

        :param process: Process instance
        :param schema: Process schema to migrate
        :param from_state: Database model state
        :return: True if the process was migrated, False otherwise
        """
        raise NotImplementedError

    def migrate_data(self, data, from_state):
        """Migrate data objects.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """
        raise NotImplementedError

    def describe_process_migration(self, process):
        """Return process migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param process: Migrated Process object instance
        """
        return {}

    def describe_data_migration(self, data):
        """Return data migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param data: Migrated Data object instance
        """
        return {}

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        """Perform forward migration."""
        Process = from_state.apps.get_model('flow', 'Process')  # pylint: disable=invalid-name
        Data = from_state.apps.get_model('flow', 'Data')  # pylint: disable=invalid-name
        try:
            # pylint: disable=invalid-name
            ProcessMigrationHistory = from_state.apps.get_model('flow', 'ProcessMigrationHistory')
            DataMigrationHistory = from_state.apps.get_model('flow', 'DataMigrationHistory')
        except LookupError:
            raise LookupError(
                "Unable to retrieve migration history models. Perhaps you need "
                "to add a migration dependency to a recent enough Resolwe flow "
                "app in your migration?"
            )

        # Migrate processes.
        processes = Process.objects.filter(slug=self.process)
        if not processes.exists():
            return

        migrated_processes = set()
        schema_field = '{}_schema'.format(self.schema_type)
        for process in processes:
            current_schema = getattr(process, schema_field)
            if not self.migrate_process_schema(process, current_schema, from_state):
                continue

            setattr(process, schema_field, current_schema)
            process.save()
            migrated_processes.add(process)

            # Update process migration log.
            ProcessMigrationHistory.objects.create(
                migration=self.migration_id,
                process=process,
                metadata=self.describe_process_migration(process),
            )

        if not migrated_processes:
            return

        # Migrate all data objects.
        data = Data.objects.filter(process__in=migrated_processes)
        self.migrate_data(data, from_state)

        # Update data migration log.
        for datum in data:
            DataMigrationHistory.objects.create(
                migration=self.migration_id,
                data=datum,
                metadata=self.describe_data_migration(datum)
            )

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        """Backward migration not possible."""
        raise NotImplementedError


class ResolweProcessAddField(ResolweProcessOperation):  # pylint: disable=abstract-method
    """Operation, which adds a new field to existing process schema."""

    def __init__(self, process, field, schema, default=None):
        """Construct a process field add operation.

        :param process: Process slug
        :param field: Dot-separated field path, starting with the top-level
            database field name (must be either ``input`` or ``output``)
        :param schema: Field schema definition
        :param default: An operation that generates default values, which
            must be an instance of :class:`DataDefaultOperation` or its
            subclass
        """
        self._raw_field = field
        self.field = field.split('.')
        if self.field[0] not in ('input', 'output'):
            raise ValueError("Field path must start with either input or output")
        if len(self.field) < 2:
            raise ValueError("Field path must contain at least two levels")
        if len(self.field) != 2:
            # TODO: Support nested fields.
            raise NotImplementedError("Field path must contain exactly two levels")
        schema_type = self.field.pop(0)

        self.schema = schema
        jsonschema.validate([schema], FIELD_SCHEMA)

        if schema['name'] != self.field[-1]:
            raise ValueError("Field name in schema differs from field path")

        if not default and schema.get('required', True):
            raise ValueError("A required field must have a default")
        if default and not isinstance(default, DataDefaultOperation):
            raise TypeError("Default must be an instance of DataDefaultOperation")
        self.default = default

        super().__init__(
            process=process,
            schema_type=schema_type,
            field=field,
            schema=schema,
            default=default
        )

    def deconstruct(self):
        """Deconstruct operation."""
        return (
            self.__class__.__name__,
            [],
            {
                'process': self.process,
                'field': self._raw_field,
                'schema': self.schema,
                'default': self.default,
            }
        )

    def migrate_process_schema(self, process, schema, from_state):
        """Migrate process schema.

        :param process: Process instance
        :param schema: Process schema to migrate
        :param from_state: Database model state
        :return: True if the process was migrated, False otherwise
        """
        container = dict_dot(schema, '.'.join(self.field[:-1]), default=list)

        # Ignore processes, which already contain the target field with the
        # target schema.
        for field in container:
            if field['name'] == self.field[-1]:
                if field == self.schema:
                    return False
                else:
                    raise ValueError(
                        "Failed to migrate schema for process '{process}' as the field '{field}' "
                        "already exists and has an incompatible schema".format(
                            process=process.slug,
                            field=self.field[-1]
                        )
                    )

        # Add field to container.
        container.append(self.schema)
        return True

    def migrate_data(self, data, from_state):
        """Migrate data objects.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """
        if not self.default:
            return

        self.default.prepare(data, from_state)
        for instance in data:
            value = self.default.get_default_for(instance, from_state)
            if not value and not self.schema.get('required', True):
                continue

            # Set default value.
            container = getattr(instance, self.schema_type, {})
            dict_dot(container, '.'.join(self.field), value)
            setattr(instance, self.schema_type, container)
            instance.save()

    def describe(self):
        """Migration description."""
        return "Add Resolwe process field"

    def describe_process_migration(self, process):
        """Return process migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param process: Migrated Process object instance
        """
        return {
            'operation': 'add_field',
            'field': self._raw_field,
        }

    def describe_data_migration(self, data):
        """Return data migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param data: Migrated Data object instance
        """
        return {
            'operation': 'add_field',
            'field': self._raw_field,
        }


class ResolweProcessRenameField(ResolweProcessOperation):  # pylint: disable=abstract-method
    """Operation, which renames an existing field in existing process schema."""

    def __init__(self, process, field, new_field, skip_no_field=False):
        """Construct a process field rename operation.

        :param process: Process slug
        :param field: Dot-separated field path, starting with the top-level
            database field name (must be either ``input`` or ``output``)
        :param new_field: New field name
        """
        self._raw_field = field
        self.field = field.split('.')
        if self.field[0] not in ('input', 'output'):
            raise ValueError("Field path must start with either input or output")
        if len(self.field) < 2:
            raise ValueError("Field path must contain at least two levels")
        if len(self.field) != 2:
            # TODO: Support nested fields.
            raise NotImplementedError("Field path must contain exactly two levels")

        schema_type = self.field.pop(0)

        self.new_field = new_field
        self.skip_no_field = skip_no_field

        super().__init__(
            process=process,
            schema_type=schema_type,
        )

    def deconstruct(self):
        """Deconstruct operation."""
        return (
            self.__class__.__name__,
            [],
            {
                'process': self.process,
                'field': self._raw_field,
                'new_field': self.new_field,
            }
        )

    def migrate_process_schema(self, process, schema, from_state):
        """Migrate process schema.

        :param process: Process instance
        :param schema: Process schema to migrate
        :param from_state: Database model state
        :return: True if the process was migrated, False otherwise
        """
        container = dict_dot(schema, '.'.join(self.field[:-1]), default=list)

        # Ignore processes, which already contain the target field.
        migrate = False
        for field in container:
            if field['name'] == self.field[-1]:
                field['name'] = self.new_field
                migrate = True
                break
            elif field['name'] == self.new_field:
                # Already has target field.
                migrate = False
                break
        else:
            if not self.skip_no_field:
                raise ValueError(
                    "Unable to rename: there is no field with name '{field}' or '{new_field}'.".format(
                        field=self.field[-1],
                        new_field=self.new_field,
                    )
                )

        return migrate

    def migrate_data(self, data, from_state):
        """Migrate data objects.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """
        for instance in data:
            if instance.status == 'ER':
                continue

            container = getattr(instance, self.schema_type, {})
            schema = container.pop(self.field[-1])
            container[self.new_field] = schema

            setattr(instance, self.schema_type, container)
            instance.save()

    def describe(self):
        """Migration description."""
        return "Rename Resolwe process field"

    def describe_process_migration(self, process):
        """Return process migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param process: Migrated Process object instance
        """
        return {
            'operation': 'rename_field',
            'field': self._raw_field,
            'new_field': self.new_field,
        }

    def describe_data_migration(self, data):
        """Return data migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param data: Migrated Data object instance
        """
        return {
            'operation': 'rename_field',
            'field': self._raw_field,
            'new_field': self.new_field,
        }


class ResolweDataCleanup(base.Operation):
    """Resolwe temporary and errored Data object removal operation."""

    reduces_to_sql = False
    atomic = True
    reversible = False

    def state_forwards(self, app_label, state):
        """Operation does not alter database state, only data."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        """Perform forward migration."""
        from resolwe.flow.models.data import DataQuerySet
        # pylint: disable=protected-access

        Data = from_state.apps.get_model('flow', 'Data')  # pylint: disable=invalid-name
        DataQuerySet._delete_chunked(Data.objects.filter(process__persistence='TMP'))
        DataQuerySet._delete_chunked(Data.objects.filter(status='ER'))

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        """Backward migration not possible."""
        raise NotImplementedError


class ResolweProcessDataRemove(base.Operation):
    """Resolwe remove Process and Data operation."""

    reduces_to_sql = False
    atomic = True
    reversible = False

    def __init__(self, process):
        """
        Remove a Process and all of it's Data.

        :param process: Process slug
        """
        self.process = process

    def state_forwards(self, app_label, state):
        """Operation does not alter database state, only data."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        """Perform forward migration."""
        from resolwe.flow.models.data import DataQuerySet
        # pylint: disable=protected-access

        Data = from_state.apps.get_model('flow', 'Data')  # pylint: disable=invalid-name
        DataQuerySet._delete_chunked(Data.objects.filter(process__slug=self.process))

        Process = from_state.apps.get_model('flow', 'Process')  # pylint: disable=invalid-name
        Process.objects.filter(slug=self.process).delete()

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        """Backward migration not possible."""
        raise NotImplementedError


class ResolweProcessChangeType(ResolweProcessOperation):  # pylint: disable=abstract-method
    """Operation, changes a process' type."""

    def __init__(self, process, new_type):
        """Construct a process type change operation.

        :param process: Process slug
        :param new_type: New process data type
        """
        self.new_type = new_type
        # TODO: Validate type.

        super().__init__(
            process=process,
            schema_type='output',
        )

    def deconstruct(self):
        """Deconstruct operation."""
        return (
            self.__class__.__name__,
            [],
            {
                'process': self.process,
                'new_type': self.new_type,
            }
        )

    def migrate_process_schema(self, process, schema, from_state):
        """Migrate process schema.

        :param process: Process instance
        :param schema: Process schema to migrate
        :param from_state: Database model state
        :return: True if the process was migrated, False otherwise
        """
        if process.type == self.new_type:
            return False

        process.type = self.new_type
        return True

    def migrate_data(self, data, from_state):
        """Migrate data objects.

        :param data: Queryset containing all data objects that need
            to be migrated
        :param from_state: Database model state
        """

    def describe(self):
        """Migration description."""
        return "Change Resolwe process type"

    def describe_process_migration(self, process):
        """Return process migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param process: Migrated Process object instance
        """
        return {
            'operation': 'change_type',
            'new_type': self.new_type,
        }

    def describe_data_migration(self, data):
        """Return data migration metadata.

        These metadata will be recorded in the migration history and
        must be JSON-serializable. The default implementation returns
        an empty dictionary.

        :param data: Migrated Data object instance
        """
        return {
            'operation': 'change_type',
            'new_type': self.new_type,
        }


class ResolweValidateProcessSchema(base.Operation):
    """Process schema validation operation."""

    reduces_to_sql = False
    atomic = True
    reversible = False

    def state_forwards(self, app_label, state):
        """Operation does not alter database state, only data."""

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        """Perform forward migration."""
        Process = from_state.apps.get_model('flow', 'Process')  # pylint: disable=invalid-name

        # Validate process types to ensure consistency.
        errors = validate_process_types(Process.objects.all())
        if errors:
            raise ValueError("Process type consistency check failed after migration: {}".format(errors))

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        """Backward migration not possible."""
        raise NotImplementedError
