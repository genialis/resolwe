""".. Ignore pydocstyle D400.

========================
Collect Processes' tools
========================

"""
import os
import shutil

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from resolwe.flow.utils import get_apps_tools


class Command(BaseCommand):
    """Copies tools' files from different locations to the settings.TOOLS_ROOT."""

    help = "Collect tools' files in a single location."

    def __init__(self, *args, **kwargs):
        """Initialize command."""
        super().__init__(*args, **kwargs)
        self.interactive = None
        self.clear = None

        if not hasattr(settings, 'FLOW_TOOLS_ROOT'):
            raise CommandError("FLOW_TOOLS_ROOT must be defined in Django settings.")

        self.destination_path = settings.FLOW_TOOLS_ROOT

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument(
            '--noinput', '--no-input', action='store_false', dest='interactive',
            help="Do NOT prompt the user for input of any kind.",
        )

        parser.add_argument(
            '-c', '--clear', action='store_true', dest='clear',
            help="Clear existing files before copying new files.",
        )

    def set_options(self, **options):
        """Set instance variables based on an options dict."""
        self.interactive = options['interactive']
        self.clear = options['clear']

    def get_confirmation(self):
        """Get user confirmation to proceed."""
        if self.clear:
            action = 'This will DELETE ALL FILES in this location!'
        else:
            action = 'This will overwrite existing files!'

        message = (
            "\n"
            "You have requested to collect static files at the destination\n"
            "location as specified in your settings\n"
            "\n"
            "    {destination}\n"
            "\n"
            "{action}\n"
            "Are you sure you want to do this?\n"
            "\n"
            "Type 'yes' to continue, or 'no' to cancel: ".format(
                destination=self.destination_path,
                action=action,
            )
        )

        if input(''.join(message)) != 'yes':
            raise CommandError("Collecting tools cancelled.")

    def clear_dir(self):
        """Delete contents of the directory on the given path."""
        self.stdout.write("Deleting contents of '{}'.".format(self.destination_path))

        for filename in os.listdir(self.destination_path):
            if os.path.isfile(filename) or os.path.islink(filename):
                os.remove(filename)
            elif os.path.isdir(filename):
                shutil.rmtree(filename)

    def change_path_prefix(self, path, old_prefix, new_prefix, app_name):
        """Change path prefix and include app name."""
        relative_path = os.path.relpath(path, old_prefix)
        return os.path.join(new_prefix, app_name, relative_path)

    def collect(self):
        """Get tools' locations and copy them to a single location."""
        for app_name, tools_path in get_apps_tools().items():
            self.stdout.write("Copying files from '{}'.".format(tools_path))

            app_name = app_name.replace('.', '_')

            app_destination_path = os.path.join(self.destination_path, app_name)
            if not os.path.isdir(app_destination_path):
                os.mkdir(app_destination_path)

            for root, dirs, files in os.walk(tools_path):
                for dir_name in dirs:
                    dir_source_path = os.path.join(root, dir_name)
                    dir_destination_path = self.change_path_prefix(
                        dir_source_path, tools_path, self.destination_path, app_name
                    )

                    if not os.path.isdir(dir_destination_path):
                        os.mkdir(dir_destination_path)

                for file_name in files:
                    file_source_path = os.path.join(root, file_name)
                    file_destination_path = self.change_path_prefix(
                        file_source_path, tools_path, self.destination_path, app_name
                    )

                    shutil.copy2(file_source_path, file_destination_path)

    def handle(self, **options):
        """Collect tools."""
        self.set_options(**options)

        os.makedirs(self.destination_path, exist_ok=True)

        if self.interactive and any(os.listdir(self.destination_path)):
            self.get_confirmation()

        if self.clear:
            self.clear_dir()

        self.collect()
