""".. Ignore pydocstyle D400.

==================
Register Processes
==================

"""
import os
import re

import jsonschema
import yaml
from versionfield.utils import convert_version_string_to_int

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand
from django.db.models import Max, Q

from resolwe.flow.engine import InvalidEngineError
from resolwe.flow.finders import get_finders
from resolwe.flow.managers import manager
from resolwe.flow.models import DescriptorSchema, Process
from resolwe.flow.models.base import VERSION_NUMBER_BITS
from resolwe.flow.models.utils import validate_schema, validation_schema
from resolwe.flow.utils import dict_dot, iterate_schema
from resolwe.permissions.utils import assign_contributor_permissions, copy_permissions

PROCESSOR_SCHEMA = validation_schema('processor')
DESCRIPTOR_SCHEMA = validation_schema('descriptor')

SCHEMA_TYPE_DESCRIPTOR = 'descriptor'
SCHEMA_TYPE_PROCESS = 'process'


class Command(BaseCommand):
    """Register processes."""

    help = 'Register processes'

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument('-f', '--force', action='store_true', help="register also if version mismatch")
        parser.add_argument('--retire', default=False, action='store_true', help="retire obsolete processes")

    def valid(self, instance, schema):
        """Validate schema."""
        try:
            jsonschema.validate(instance, schema)
        except jsonschema.exceptions.ValidationError as ex:
            self.stderr.write("    VALIDATION ERROR: {}".format(instance['name'] if 'name' in instance else ''))
            self.stderr.write("        path:       {}".format(ex.path))
            self.stderr.write("        message:    {}".format(ex.message))
            self.stderr.write("        validator:  {}".format(ex.validator))
            self.stderr.write("        val. value: {}".format(ex.validator_value))
            return False

        try:
            # Check that default values fit field schema.
            for field in ['input', 'output', 'schema']:
                for schema, _, path in iterate_schema({}, instance.get(field, {})):
                    if 'default' in schema:
                        validate_schema({schema['name']: schema['default']}, [schema])
        except ValidationError:
            self.stderr.write("    VALIDATION ERROR: {}".format(instance['name']))
            self.stderr.write("        Default value of field '{}' is not valid.". format(path))
            return False

        return True

    def find_descriptor_schemas(self, schema_file):
        """Find descriptor schemas in given path."""
        if not schema_file.lower().endswith(('.yml', '.yaml')):
            return []

        with open(schema_file) as fn:
            schemas = yaml.load(fn)
        if not schemas:
            self.stderr.write("Could not read YAML file {}".format(schema_file))
            return []

        descriptor_schemas = []
        for schema in schemas:
            if 'schema' not in schema:
                continue

            descriptor_schemas.append(schema)

        return descriptor_schemas

    def find_schemas(self, schema_path, schema_type=SCHEMA_TYPE_PROCESS, verbosity=1):
        """Find schemas in packages that match filters."""
        schema_matches = []

        if not os.path.isdir(schema_path):
            if verbosity > 0:
                self.stdout.write("Invalid path {}".format(schema_path))
            return

        if schema_type not in [SCHEMA_TYPE_PROCESS, SCHEMA_TYPE_DESCRIPTOR]:
            raise ValueError('Invalid schema type')

        for root, _, files in os.walk(schema_path):
            for schema_file in [os.path.join(root, fn) for fn in files]:
                schemas = None
                if schema_type == SCHEMA_TYPE_DESCRIPTOR:
                    # Discover descriptors.
                    schemas = self.find_descriptor_schemas(schema_file)
                elif schema_type == SCHEMA_TYPE_PROCESS:
                    # Perform process discovery for all supported execution engines.
                    schemas = []
                    for execution_engine in manager.execution_engines.values():
                        schemas.extend(execution_engine.discover_process(schema_file))

                for schema in schemas:
                    schema_matches.append(schema)

        return schema_matches

    def register_processes(self, process_schemas, user, force=False, verbosity=1):
        """Read and register processors."""
        log_processors = []
        log_templates = []

        for p in process_schemas:
            # TODO: Remove this when all processes are migrated to the
            #       new syntax.
            if 'flow_collection' in p:
                if 'entity' in p:
                    self.stderr.write(
                        "Skip processor {}: only one of 'flow_collection' and 'entity' fields "
                        "allowed".format(p['slug'])
                    )
                    continue

                p['entity'] = {'type': p.pop('flow_collection')}

            if p['type'][-1] != ':':
                p['type'] += ':'

            if 'category' in p and not p['category'].endswith(':'):
                p['category'] += ':'

            for field in ['input', 'output']:
                for schema, _, _ in iterate_schema({}, p[field] if field in p else {}):
                    if not schema['type'][-1].endswith(':'):
                        schema['type'] += ':'
            # TODO: Check if schemas validate with our JSON meta schema and Processor model docs.

            if not self.valid(p, PROCESSOR_SCHEMA):
                continue

            if 'entity' in p:
                if 'type' not in p['entity']:
                    self.stderr.write(
                        "Skip process {}: 'entity.type' required if 'entity' defined".format(p['slug'])
                    )
                    continue

                p['entity_type'] = p['entity']['type']
                p['entity_descriptor_schema'] = p['entity'].get('descriptor_schema', p['entity_type'])
                p['entity_input'] = p['entity'].get('input', None)
                p.pop('entity')

                if not DescriptorSchema.objects.filter(slug=p['entity_descriptor_schema']).exists():
                    self.stderr.write(
                        "Skip processor {}: Unknown descriptor schema '{}' used in 'entity' "
                        "field.".format(p['slug'], p['entity_descriptor_schema'])
                    )
                    continue

            if 'persistence' in p:
                persistence_mapping = {
                    'RAW': Process.PERSISTENCE_RAW,
                    'CACHED': Process.PERSISTENCE_CACHED,
                    'TEMP': Process.PERSISTENCE_TEMP,
                }

                p['persistence'] = persistence_mapping[p['persistence']]

            if 'scheduling_class' in p:
                scheduling_class_mapping = {
                    'interactive': Process.SCHEDULING_CLASS_INTERACTIVE,
                    'batch': Process.SCHEDULING_CLASS_BATCH
                }

                p['scheduling_class'] = scheduling_class_mapping[p['scheduling_class']]

            if 'input' in p:
                p['input_schema'] = p.pop('input')

            if 'output' in p:
                p['output_schema'] = p.pop('output')

            slug = p['slug']

            if 'run' in p:
                # Set default language to 'bash' if not set.
                p['run'].setdefault('language', 'bash')

                # Transform output schema using the execution engine.
                try:
                    execution_engine = manager.get_execution_engine(p['run']['language'])
                    extra_output_schema = execution_engine.get_output_schema(p)
                    if extra_output_schema:
                        p.setdefault('output_schema', []).extend(extra_output_schema)
                except InvalidEngineError:
                    self.stderr.write("Skip processor {}: execution engine '{}' not supported".format(
                        slug, p['run']['language']
                    ))
                    continue

            # Validate if container image is allowed based on the configured pattern.
            # NOTE: This validation happens here and is not deferred to executors because the idea
            #       is that this will be moved to a "container" requirement independent of the
            #       executor.
            if hasattr(settings, 'FLOW_CONTAINER_VALIDATE_IMAGE'):
                try:
                    container_image = dict_dot(p, 'requirements.executor.docker.image')
                    if not re.match(settings.FLOW_CONTAINER_VALIDATE_IMAGE, container_image):
                        self.stderr.write("Skip processor {}: container image does not match '{}'".format(
                            slug, settings.FLOW_CONTAINER_VALIDATE_IMAGE,
                        ))
                        continue
                except KeyError:
                    pass

            version = p['version']
            int_version = convert_version_string_to_int(version, VERSION_NUMBER_BITS)

            # `latest version` is returned as `int` so it has to be compared to `int_version`
            latest_version = Process.objects.filter(slug=slug).aggregate(Max('version'))['version__max']
            if latest_version is not None and latest_version > int_version:
                self.stderr.write("Skip processor {}: newer version installed".format(slug))
                continue

            previous_process_qs = Process.objects.filter(slug=slug)
            if previous_process_qs.exists():
                previous_process = previous_process_qs.latest()
            else:
                previous_process = None

            process_query = Process.objects.filter(slug=slug, version=version)
            if process_query.exists():
                if not force:
                    if verbosity > 0:
                        self.stdout.write("Skip processor {}: same version installed".format(slug))
                    continue

                process_query.update(**p)
                log_processors.append("Updated {}".format(slug))
            else:
                process = Process.objects.create(contributor=user, **p)
                assign_contributor_permissions(process)
                if previous_process:
                    copy_permissions(previous_process, process)
                log_processors.append("Inserted {}".format(slug))

        if verbosity > 0:
            if log_processors:
                self.stdout.write("Processor Updates:")
                for log in log_processors:
                    self.stdout.write("  {}".format(log))

            if log_templates:
                self.stdout.write("Default Template Updates:")
                for log in log_templates:
                    self.stdout.write("  {}".format(log))

    def register_descriptors(self, descriptor_schemas, user, force=False, verbosity=1):
        """Read and register descriptors."""
        log_descriptors = []

        for descriptor_schema in descriptor_schemas:
            for schema, _, _ in iterate_schema({}, descriptor_schema.get('schema', {})):
                if not schema['type'][-1].endswith(':'):
                    schema['type'] += ':'

            if 'schema' not in descriptor_schema:
                descriptor_schema['schema'] = []

            if not self.valid(descriptor_schema, DESCRIPTOR_SCHEMA):
                continue

            slug = descriptor_schema['slug']
            version = descriptor_schema.get('version', '0.0.0')
            int_version = convert_version_string_to_int(version, VERSION_NUMBER_BITS)

            # `latest version` is returned as `int` so it has to be compared to `int_version`
            latest_version = DescriptorSchema.objects.filter(slug=slug).aggregate(Max('version'))['version__max']
            if latest_version is not None and latest_version > int_version:
                self.stderr.write("Skip descriptor schema {}: newer version installed".format(slug))
                continue

            previous_descriptor_qs = DescriptorSchema.objects.filter(slug=slug)
            if previous_descriptor_qs.exists():
                previous_descriptor = previous_descriptor_qs.latest()
            else:
                previous_descriptor = None

            descriptor_query = DescriptorSchema.objects.filter(slug=slug, version=version)
            if descriptor_query.exists():
                if not force:
                    if verbosity > 0:
                        self.stdout.write("Skip descriptor schema {}: same version installed".format(slug))
                    continue

                descriptor_query.update(**descriptor_schema)
                log_descriptors.append("Updated {}".format(slug))
            else:
                descriptor = DescriptorSchema.objects.create(contributor=user, **descriptor_schema)
                assign_contributor_permissions(descriptor)
                if previous_descriptor:
                    copy_permissions(previous_descriptor, descriptor)
                log_descriptors.append("Inserted {}".format(slug))

        if log_descriptors and verbosity > 0:
            self.stdout.write("Descriptor schemas Updates:")
            for log in log_descriptors:
                self.stdout.write("  {}".format(log))

    def retire(self, process_schemas):
        """Retire obsolete processes.

        Remove old process versions without data. Find processes that have been
        registered but do not exist in the code anymore, then:

        - If they do not have data: remove them
        - If they have data: flag them not active (``is_active=False``)

        """
        process_slugs = set(ps['slug'] for ps in process_schemas)

        # Processes that are in DB but not in the code
        retired_processes = Process.objects.filter(~Q(slug__in=process_slugs))

        # Remove retired processes which do not have data
        retired_processes.filter(data__exact=None).delete()

        # Remove non-latest processes which do not have data
        latest_version_processes = Process.objects.order_by('slug', '-version').distinct('slug')
        Process.objects.filter(data__exact=None).difference(latest_version_processes).delete()

        # Deactivate retired processes which have data
        retired_processes.update(is_active=False)

    def handle(self, *args, **options):
        """Register processes."""
        force = options.get('force')
        retire = options.get('retire')
        verbosity = int(options.get('verbosity'))

        users = get_user_model().objects.filter(is_superuser=True).order_by('date_joined')

        if not users.exists():
            self.stderr.write("Admin does not exist: create a superuser")
            exit(1)

        process_paths, descriptor_paths = [], []
        process_schemas, descriptor_schemas = [], []

        for finder in get_finders():
            process_paths.extend(finder.find_processes())
            descriptor_paths.extend(finder.find_descriptors())

        for proc_path in process_paths:
            process_schemas.extend(
                self.find_schemas(proc_path, schema_type=SCHEMA_TYPE_PROCESS, verbosity=verbosity))

        for desc_path in descriptor_paths:
            descriptor_schemas.extend(
                self.find_schemas(desc_path, schema_type=SCHEMA_TYPE_DESCRIPTOR, verbosity=verbosity))

        user_admin = users.first()
        self.register_descriptors(descriptor_schemas, user_admin, force, verbosity=verbosity)
        # NOTE: Descriptor schemas must be registered first, so
        #       processes can validate 'entity_descriptor_schema' field.
        self.register_processes(process_schemas, user_admin, force, verbosity=verbosity)

        if retire:
            self.retire(process_schemas)

        if verbosity > 0:
            self.stdout.write("Running executor post-registration hook...")
        manager.get_executor().post_register_hook(verbosity=verbosity)
