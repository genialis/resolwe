"""Register processes"""
from __future__ import absolute_import, division, print_function, unicode_literals

import jsonschema
import os
import yaml

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.db.models import Max

from resolwe.flow.models import Process, iterate_schema, validation_schema


PROCESSOR_SCHEMA = validation_schema('processor')
VAR_SCHEMA = validation_schema('descriptor')


class Command(BaseCommand):

    """Register processes"""

    help = 'Register processes'

    def add_arguments(self, parser):
        parser.add_argument('-s', '--schemas', type=str, nargs='*', help="process names to register")
        parser.add_argument('-f', '--force', action='store_true', help="register also if version mismatch")
        parser.add_argument('--path', help="path to look for processes")

    def valid(self, instance, schema):
        """Validate schema."""
        try:
            jsonschema.validate(instance, schema)
            return True
        except jsonschema.exceptions.ValidationError as ex:
            self.stderr.write("    VALIDATION ERROR: {}".format(instance['name'] if 'name' in instance else ''))
            self.stderr.write("        path:       {}".format(ex.path))
            self.stderr.write("        message:    {}".format(ex.message))
            self.stderr.write("        validator:  {}".format(ex.validator))
            self.stderr.write("        val. value: {}".format(ex.validator_value))
            return False

    def find_schemas(self, schema_path, filters=None):
        """Find schemas in packages that match filters."""
        schema_matches = []

        if not os.path.isdir(schema_path):
            self.stdout.write("Invalid path {}".format(schema_path))
            return

        for filename in os.listdir(schema_path):
            if not filename.endswith('.yml') and not filename.endswith('.yaml'):
                continue

            schema_file = os.path.join(schema_path, filename)
            schemas = yaml.load(open(schema_file))
            if not schemas:
                self.stderr.write("Could not read YAML file {}".format(schema_file))
                continue

            schema_matches.extend(schema for schema in schemas if
                                  not filters or schema.get('name', None) in filters or
                                  schema.get('slug', None) in filters)

        return schema_matches

    def register_processes(self, process_schemas, user, force=False):
        """Read and register processors."""
        log_processors = []
        log_templates = []

        for p in process_schemas:
            # Handle backwards compatiblity
            if 'slug' not in p:
                p['slug'] = p['name']
                p['name'] = p['label']

            if p['type'][-1] != ':':
                p['type'] += ':'

            if 'category' in p and p['category'][-1] != ':':
                p['category'] += ':'

            for field in ['input', 'output', 'var', 'static']:
                for schema, _, _ in iterate_schema({}, p[field] if field in p else {}):
                    if schema['type'][-1] != ':':
                        schema['type'] += ':'

            if not self.valid(p, PROCESSOR_SCHEMA):
                continue

            slug = p['slug']
            version = p['version']

            max_version_query = Process.objects.filter(slug=slug).aggregate(Max('version'))
            if max_version_query['version__max'] is not None:
                if max_version_query['version__max'] > version:
                    self.stderr.write("Skip processor {}: newer version installed".format(slug))
                    continue

            try:
                process = Process.objects.get(slug=slug, version=version)
                if not force:
                    self.stdout.write("Skip processor {}: same version installed".format(slug))
                    continue

                log_processors.append("Updated {}".format(slug))

            except Process.DoesNotExist:
                process = Process()
                process.slug = slug
                process.contributor = user
                log_processors.append("Inserted {}".format(slug))

            process.name = p['name']
            process.type = p['type']
            process.version = version

            if 'description' in p:
                process.description = p['description']

            if 'category' in p:
                process.category = p['category']

            if 'persistence' in p:
                persistence = {
                    'RAW': Process.PERSISTENCE_RAW,
                    'CACHED': Process.PERSISTENCE_CACHED,
                    'TEMP': Process.PERSISTENCE_TEMP,
                }

                process.persistence = persistence[p['persistence']]

            # TODO: Check if schemas validate with our JSON meta schema and Processor model docs.
            process.input_schema = p.get('input', [])
            process.output_schema = p.get('output', [])
            process.adapter = p['run']['bash']
            process.save()

        if len(log_processors) > 0:
            self.stdout.write("Processor Updates:")
            for log in log_processors:
                self.stdout.write("  {}".format(log))

        if len(log_templates) > 0:
            self.stdout.write("Default Template Updates:")
            for log in log_templates:
                self.stdout.write("  {}".format(log))

    def handle(self, *args, **options):
        schemas = options.get('schemas')
        path = options.get('path')
        force = options.get('force')

        if not path:
            raise NotImplementedError("Give path to processes folder (--path)")

        users = get_user_model().objects.filter(is_superuser=True).order_by('date_joined')

        if not users.exists():
            self.stderr.write("Admin does not exist: create a superuser")
            exit(1)

        user_admin = users.first()

        # package_schemas = self.find_packages(schemas, path)

        process_schemas = self.find_schemas(path, schemas)
        self.register_processes(process_schemas, user_admin, force)
