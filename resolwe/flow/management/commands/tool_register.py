"""Register tools"""
from __future__ import absolute_import, division, print_function, unicode_literals

import jsonschema
import os
import yaml

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model

from resolwe.flow.models import Tool, iterate_schema, validation_schema


PROCESSOR_SCHEMA = validation_schema('processor')
VAR_SCHEMA = validation_schema('annotation')


class Command(BaseCommand):

    """Register tools"""

    help = 'Register tools'

    def add_arguments(self, parser):
        parser.add_argument('-s', '--schemas', type=str, nargs='*',
                            help="tool names to register")
        parser.add_argument('-f', '--force', action='store_true', help="register also if version mismatch")
        parser.add_argument('--path', help="path to look for tools")

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

    def find_schemas(self, subdir, filters=None, genpackages_path=None):
        """Find schemas in packages that match filters.

        To find processors use `processors` for `subdir`.
        To find templates use `var_templates` for `subdir`.

        """
        schema_matches = []

        if not genpackages_path:
            raise NotImplementedError()
            # genpackages_path = os.path.join(settings.PROJECT_ROOT, 'genpackages')

        schema_paths = [os.path.join(genpackages_path, name, subdir) for name in os.listdir(genpackages_path)]
        schema_paths = [schema_path for schema_path in schema_paths if os.path.isdir(schema_path)]

        for schema_path in schema_paths:
            if not os.path.isdir(schema_path):
                self.stdout.write("App {} does not have a {} directory.".format(schema_path, subdir))
                continue

            for filename in os.listdir(schema_path):
                if not filename.endswith('.yml') and not filename.endswith('.yaml'):
                    continue

                schema_file = os.path.join(schema_path, filename)
                schemas = yaml.load(open(schema_file))
                if not schemas:
                    self.stderr.write("Could not read YAML file {}".format(schema_file))
                    continue

                schema_matches.extend(schema for schema in schemas if
                                      not filters or schema.get('name', None) in filters)

        return schema_matches

    def find_packages(self, filters=None, genpackages_path=None):
        """Find package schemas in GenPackages that match the filters."""
        schema_matches = {}

        if not genpackages_path:
            raise NotImplementedError()
            # genpackages_path = os.path.join(settings.PROJECT_ROOT, 'genpackages')

        schema_files = [os.path.join(genpackages_path, name) for name in os.listdir(genpackages_path)]
        schema_files = [os.path.join(path, 'package.yml') for path in schema_files if os.path.isdir(path)]

        for schema_file in schema_files:
            if not os.path.isfile(schema_file):
                self.stdout.write("No package.yml found in {}".format(os.path.dirname(schema_file)))
                continue

            schema = yaml.load(open(schema_file))
            if not schema:
                self.stderr.write("Could not read YAML file {}".format(schema_file))
                continue

            name = os.path.basename(os.path.dirname(schema_file))
            if not filters or name in filters:
                schema_matches[schema_file] = schema

        return schema_matches

    def register_tools(self, tool_schemas, force=False, user=None):
        """Read and register processors."""
        log_processors = []
        log_templates = []

        for p in tool_schemas:
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

            slug = p['name']
            version = int(''.join('0' * (3 - len(v)) + v for v in p['version'].split('.')))

            try:
                tool = Tool.objects.get(slug=slug)

                if tool.version > version:
                    self.stderr.write("Skip processor {} - newer version installed.".format(slug))
                    continue

                elif tool.version == version and not force:
                    self.stdout.write("Skip processor {} - same version installed.".format(slug))
                    continue

                log_processors.append("Updated {}".format(slug))

            except Tool.DoesNotExist:
                tool = Tool()
                tool.slug = slug
                tool.contributor = user
                log_processors.append("Inserted {}".format(slug))

            tool.name = p['label']
            tool.type = p['type']
            tool.version = version
            tool.description = p['description']

            if 'category' in p:
                tool.category = p['category']

            persistence = {
                'RAW': Tool.PERSISTENCE_RAW,
                'CACHED': Tool.PERSISTENCE_CACHED,
                'TEMP': Tool.PERSISTENCE_TEMP,
            }

            tool.persistence = persistence[p['persistence']]

            # TODO: Check if schemas validate with our JSON meta schema and Processor model docs.
            tool.input_schema = p['input'] if 'input' in p else []
            tool.output_schema = p['output'] if 'output' in p else []
            tool.adapter = p['run']
            tool.save()

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

        users = get_user_model().objects.filter(is_superuser=True).order_by('date_joined')

        if len(users) == 0:
            self.stderr.write("Admin does not exist: create a superuser")
            exit(1)

        user_admin = users[0]

        # package_schemas = self.find_packages(schemas, path)

        tool_schemas = self.find_schemas('processors', schemas, path)
        self.register_tools(tool_schemas, force, user_admin)
