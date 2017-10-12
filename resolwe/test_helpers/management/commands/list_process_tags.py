""".. Ignore pydocstyle D400.

======================
List Process Test Tags
======================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import re

import yaml

from django.core.management.base import BaseCommand

from resolwe.flow.finders import get_finders
from resolwe.test.utils import generate_process_tag

SPAWN_PROCESS_REGEX = re.compile(r'run\s+\{.*?["\']process["\']\s*:\s*["\'](.+?)["\'].*?\}')


class Command(BaseCommand):
    """Utility for listing process test tags of specific files."""

    help = "Utility for listing process test tags of specific files."

    def add_arguments(self, parser):
        """Add command arguments."""
        parser.add_argument('files', nargs='+', type=str,
                            help="process definition YAML file(s)")

    def find_schemas(self, schema_path):
        """Find process schemas."""
        schema_matches = []

        for root, _, files in os.walk(schema_path):
            for schema_file in [os.path.join(root, fn) for fn in files]:

                if not schema_file.lower().endswith(('.yml', '.yaml')):
                    continue

                with open(schema_file) as fn:
                    schemas = yaml.load(fn)

                if not schemas:
                    self.stderr.write("Could not read YAML file {}".format(schema_file))
                    continue

                for schema in schemas:
                    schema_matches.append(schema)

        return schema_matches

    def find_dependencies(self, schemas):
        """Compute process dependencies."""
        dependencies = {}

        for schema in schemas:
            slug = schema['slug']
            run = schema.get('run', {})
            program = run.get('program', None)
            language = run.get('language', None)

            if language == 'workflow':
                for step in program:
                    dependencies.setdefault(step['run'], set()).add(slug)
            elif language == 'bash':
                # Process re-spawn instructions to discover dependencies.
                matches = SPAWN_PROCESS_REGEX.findall(program)
                if matches:
                    for match in matches:
                        dependencies.setdefault(match, set()).add(slug)

        return dependencies

    def handle(self, files, **options):
        """Handle command."""
        processes_paths = []
        for finder in get_finders():
            processes_paths.extend(finder.find_processes())

        process_schemas = []
        for proc_path in processes_paths:
            process_schemas.extend(self.find_schemas(proc_path))

        dependencies = self.find_dependencies(process_schemas)
        processes = set()

        for filename in files:
            with open(filename, 'r') as process_file:
                data = yaml.load(process_file)

                for process in data:
                    # Add all process slugs.
                    processes.add(process['slug'])

        # Add all dependencies.
        dep_processes = set()
        while processes:
            process = processes.pop()
            if process in dep_processes:
                continue

            dep_processes.add(process)
            processes.update(dependencies.get(process, set()))

        tags = set()
        for process in dep_processes:
            tags.add(generate_process_tag(process))

        for tag in tags:
            self.stdout.write(tag)
