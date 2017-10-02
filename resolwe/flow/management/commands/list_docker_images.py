""".. Ignore pydocstyle D400.

==================
List Docker images
==================

"""

import functools
import operator
import shlex
import subprocess

import yaml

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from resolwe.flow.models import Process


class Command(BaseCommand):
    """List Docker images used by processes.  Optionally also pull them."""

    help = "List Docker images used by processes in either plain text or YAML (for Ansible)"

    def add_arguments(self, parser):
        """Add an argument to specify output format."""
        parser.add_argument('--format', dest='format', default='plain',
                            help="Set output format ('plain' [default] or 'yaml')")
        parser.add_argument('--pull', dest='pull', default=False, action='store_true',
                            help="Pull all images with Docker")
        parser.add_argument('--ignore-pull-errors', dest='ignore_pull_errors',
                            default=getattr(settings, 'FLOW_DOCKER_IGNORE_PULL_ERRORS', False),
                            action='store_true',
                            help="Don't fail whenever a Docker image can't be pulled")

    def handle(self, *args, **options):
        """Handle command list_docker_images."""
        # Check that the specified output format is valid
        if options['format'] != 'plain' and options['format'] != 'yaml':
            raise CommandError("Unknown output format: %s" % options['format'])

        # Gather only unique latest custom Docker requirements that the processes are using
        # The 'image' field is optional, so be careful about that as well
        unique_docker_images = set(
            p.requirements['executor']['docker']['image']
            for p in Process.objects.order_by(
                'slug', '-version'
            ).distinct(
                'slug'
            ).only(
                'requirements'
            ).filter(
                requirements__icontains='docker'
            )
            if 'image' in p.requirements.get('executor', {}).get('docker', {})
        )

        # Add the default image if it exists
        if 'CONTAINER_IMAGE' in settings.FLOW_EXECUTOR:
            # This Docker image is used if a process doesn't specify its own
            default_docker_image = settings.FLOW_EXECUTOR['CONTAINER_IMAGE']

            unique_docker_images.add(default_docker_image)

        # Sort the set of unique Docker images for nicer output
        unique_docker_images = sorted(unique_docker_images)

        # Pull images if requested or just output the list in specified format
        if options['pull']:
            # Get the desired 'docker' command from settings or use the default
            docker = getattr(settings, 'FLOW_DOCKER_COMMAND', 'docker')

            # Pull each image
            for img in unique_docker_images:
                ret = subprocess.call(shlex.split('{} pull {}'.format(docker, img)))

                if ret != 0:
                    errmsg = "Failed to pull Docker image '{}'!".format(img)

                    if not options['ignore_pull_errors']:
                        # Print error and stop execution
                        raise CommandError(errmsg)
                    else:
                        # Print error, but keep going
                        self.stderr.write(errmsg)
                else:
                    self.stdout.write("Docker image '{}' pulled successfully!".format(img))
        else:
            # Convert the set of unique Docker images into a list of dicts for easier output
            imgs = [
                dict(name=s[0], tag=s[1] if len(s) == 2 else 'latest')
                for s in (img.split(':') for img in unique_docker_images)
            ]

            # Output in YAML or plaintext (one image per line), as requested
            if options['format'] == 'yaml':
                out = yaml.safe_dump(imgs, default_flow_style=True, default_style="'")
            else:
                out = functools.reduce(operator.add,
                                       ('{name}:{tag}\n'.format(**i) for i in imgs), '')

            self.stdout.write(out, ending='')
