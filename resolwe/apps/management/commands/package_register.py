"""Register packages"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import yaml

from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from django.db.models import Max

from versionfield.utils import convert_version_string_to_int

from resolwe.apps.models import Package
from resolwe.flow.models import VERSION_NUMBER_BITS


class Command(BaseCommand):

    """Register packages"""

    help = 'Register packages'

    def add_arguments(self, parser):
        parser.add_argument('-s', '--schemas', type=str, nargs='*', help="package names to register")
        parser.add_argument('-f', '--force', action='store_true', help="register also if version mismatch")
        parser.add_argument('--path', help="path to look for packages")

    def find_schemas(self, schema_path, filters=None):
        """Find schemas in packages that match filters."""
        schema_matches = {}

        schema_files = [os.path.join(schema_path, name) for name in os.listdir(schema_path)]
        schema_files = [os.path.join(path, 'package.yml') for path in schema_files if os.path.isdir(path)]

        for schema_file in schema_files:
            if not os.path.isfile(schema_file):
                print("No package.yml found in {}".format(os.path.dirname(schema_file)))
                continue

            schema = yaml.load(open(schema_file))
            if not schema:
                print("Could not read YAML file {}".format(schema_file), file=self.stderr)
                continue

            name = os.path.basename(os.path.dirname(schema_file))
            if not filters or name in filters:
                schema_matches[schema_file] = schema

        return schema_matches

    def register_packages(self, package_schemas, user, force=False):
        """Read and register processors."""
        log_packages = []

        for package_file, package_schema in package_schemas.iteritems():
            # Processor only GenPackage
            if 'modules' not in package_schema:
                continue

            package_schema['index'] = os.path.join(os.path.dirname(package_file), 'index.html')

            slug = package_schema['slug']
            version = package_schema['version']
            int_version = convert_version_string_to_int(version, VERSION_NUMBER_BITS)

            # `latest version` is returned as `int` so it has to be compared to `int_version`
            latest_version = Package.objects.filter(slug=slug).aggregate(Max('version'))['version__max']
            if latest_version is not None and latest_version > int_version:
                self.stderr.write("Skip package {}: newer version installed".format(slug))
                continue

            package_query = Package.objects.filter(slug=slug, version=version)
            if package_query.exists():
                if not force:
                    self.stdout.write("Skip package {}: same version installed".format(slug))
                    continue

                package_query.update(**package_schema)
                log_packages.append("Updated {}".format(slug))

            else:
                Package.objects.create(contributor=user, **package_schema)
                log_packages.append("Inserted {}".format(slug))

        if len(log_packages) > 0:
            self.stdout.write("Packages Updates:")
            for log in log_packages:
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

        package_schemas = self.find_schemas(path, schemas)
        self.register_packages(package_schemas, user_admin, force)
