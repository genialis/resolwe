""".. Ignore pydocstyle D400.

==================
List Docker images
==================

"""

import functools
import operator

import yaml

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from resolwe.flow.models import Process


class Command(BaseCommand):
    """List Docker images used by processes."""

    help = "List Docker images used by processes in either plain text or YAML (for Ansible)"

    def add_arguments(self, parser):
        """Add an argument to specify output format."""
        parser.add_argument('--format', dest='format', default='plain',
                            help="Set output format ('plain' [default] or 'yaml')")

    def handle(self, *args, **options):
        """Handle command list_docker_images."""
        # Check that the specified output format is valid
        if options['format'] != 'plain' and options['format'] != 'yaml':
            raise CommandError("Unknown output format: %s" % options['format'])

        # Gather a list of all custom Docker requirements the processes are using
        all_docker_requirements = [
            p.requirements['executor']['docker']
            for p in Process.objects.filter(requirements__icontains='docker')
        ]

        # Filter the list to include only unique images (also, the 'images' field is optional)
        unique_docker_images = set(i['image'] for i in all_docker_requirements if 'image' in i)

        # Add the default image if it exists
        if 'CONTAINER_IMAGE' in settings.FLOW_EXECUTOR:
            # This Docker image is used if a process doesn't specify its own
            default_docker_image = settings.FLOW_EXECUTOR['CONTAINER_IMAGE']

            unique_docker_images.add(default_docker_image)

        # Convert the set of unique Docker images into a list of dicts
        imgs = [
            dict(name=s[0], tag=s[1] if len(s) == 2 else 'latest')
            for s in (img.split(':') for img in unique_docker_images)
        ]

        # Output list in specified format
        if options['format'] == 'yaml':
            out = yaml.safe_dump(imgs, default_flow_style=True, default_style="'")
        else:
            out = functools.reduce(operator.add, ('{name}:{tag}\n'.format(**i) for i in imgs), '')

        self.stdout.write(out, ending='')
