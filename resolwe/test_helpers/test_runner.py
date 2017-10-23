""".. Ignore pydocstyle D400.

===================
Resolwe Test Runner
===================

"""
import os
import re
import subprocess

import yaml

from django.core.management.base import CommandError
from django.test.runner import DiscoverRunner

from resolwe.flow.finders import get_finders
from resolwe.test.utils import generate_process_tag

SPAWN_PROCESS_REGEX = re.compile(r'run\s+\{.*?["\']process["\']\s*:\s*["\'](.+?)["\'].*?\}')


class ResolweRunner(DiscoverRunner):
    """Resolwe test runner."""

    def __init__(self, *args, **kwargs):
        """Initialize test runner."""
        self.only_changes_to = kwargs.pop('only_changes_to', None)
        self.changes_file_types = kwargs.pop('changes_file_types', None)

        super(ResolweRunner, self).__init__(*args, **kwargs)

    @classmethod
    def add_arguments(cls, parser):
        """Add command-line arguments.

        :param parser: Argument parser instance
        """
        super(ResolweRunner, cls).add_arguments(parser)

        parser.add_argument(
            '--only-changes-to', dest='only_changes_to',
            help="Only test changes against given Git commit reference"
        )

        parser.add_argument(
            '--changes-file-types', dest='changes_file_types',
            help="File which describes what kind of changes are available"
        )

    def run_tests(self, test_labels, **kwargs):
        """Run tests.

        :param test_labels: Labels of tests to run
        """
        if self.only_changes_to:
            print("Detecting changed files between {} and HEAD.".format(self.only_changes_to))
            try:
                changed_files = subprocess.check_output(['git', 'diff', '--name-only', self.only_changes_to, 'HEAD'])
                changed_files = changed_files.decode('utf8').strip().split('\n')
                changed_files = [file for file in changed_files if file.strip()]

                top_level_path = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'])
                top_level_path = top_level_path.decode('utf8').strip()
            except subprocess.CalledProcessError:
                print("ERROR: Git command failed.")
                return False

            # Process changed files to discover what they are.
            changed_files, tags, tests, full_suite = self.process_changed_files(changed_files, top_level_path)
            print("Changed files:")
            for filename, file_type in changed_files:
                print("  {} ({})".format(filename, file_type))

            if not changed_files:
                print("  none")
                print("No files have been changed, assuming target is HEAD, running full suite.")
            elif full_suite:
                print("Non-test code or unknown files have been modified, running full test suite.")
            else:
                # Run specific tests/tags.
                self.tags = tags
                test_labels = tests

        return super(ResolweRunner, self).run_tests(test_labels, **kwargs)

    def process_changed_files(self, changed_files, top_level_path):
        """Process changed files based on specified patterns.

        :param changed_files: A list of changed file pats, relative to top-level path
        :param top_level_path: Absolute path to top-level project directory
        :return: Tuple (changed_files, tags, tests, full_suite)
        """
        result = []
        processes = []
        tests = []
        full_suite = False
        types = []

        if self.changes_file_types:
            # Parse file types metadata.
            try:
                with open(self.changes_file_types, 'r') as definition_file:
                    types = yaml.load(definition_file)
            except (OSError, ValueError):
                raise CommandError("Failed loading or parsing file types metadata.")
        else:
            print("WARNING: Treating all files as unknown because --changes-file-types option not specified.")

        for filename in changed_files:
            # Match file type.
            file_type = 'unknown'
            file_type_name = 'unknown'

            for definition in types:
                if re.search(definition['match'], filename):
                    file_type = definition['type']
                    file_type_name = definition.get('name', file_type)
                    break

            result.append((filename, file_type_name))
            if file_type in ('unknown', 'force_run'):
                full_suite = True
            elif file_type == 'ignore':
                # Ignore
                pass
            elif file_type == 'process':
                # Resolve process tag.
                processes.append(os.path.join(top_level_path, filename))
            elif file_type == 'test':
                # Generate test name.
                tests.append(re.sub(r'\.py$', '', filename).replace(os.path.sep, '.'))
            else:
                raise CommandError("Unsupported file type: {}".format(file_type))

        # Resolve tags.
        tags = self.resolve_process_tags(processes)

        return result, tags, tests, full_suite

    def find_schemas(self, schema_path):
        """Find process schemas.

        :param schema_path: Path where to look for process schemas
        :return: Found schemas
        """
        schema_matches = []

        for root, _, files in os.walk(schema_path):
            for schema_file in [os.path.join(root, fn) for fn in files]:

                if not schema_file.lower().endswith(('.yml', '.yaml')):
                    continue

                with open(schema_file) as fn:
                    schemas = yaml.load(fn)

                if not schemas:
                    print("WARNING: Could not read YAML file {}".format(schema_file))
                    continue

                for schema in schemas:
                    schema_matches.append(schema)

        return schema_matches

    def find_dependencies(self, schemas):
        """Compute process dependencies.

        :param schemas: A list of all discovered process schemas
        :return: Process dependency dictionary
        """
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

    def resolve_process_tags(self, files):
        """Resolve process tags.

        :param files: List of changed process files
        :return: Test tags that need to be run
        """
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

        return tags
