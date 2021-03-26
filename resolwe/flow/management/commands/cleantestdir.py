""".. Ignore pydocstyle D400.

====================
Clean test directory
====================

Command to run on local machine::

    ./manage.py cleantestdir

"""
import re
import shutil
from itertools import chain
from pathlib import Path

from django.core.management.base import BaseCommand

from resolwe.storage import settings as storage_settings
from resolwe.storage.connectors import connectors

TEST_DIR_REGEX = r"^test_.*_\d+$"


class Command(BaseCommand):
    """Cleanup files created during testing."""

    help = "Cleanup files created during testing."

    def handle(self, *args, **kwargs):
        """Cleanup files created during testing."""
        directories = [
            Path(connector.path)
            for connector in chain(
                connectors.for_storage("data"), connectors.for_storage("upload")
            )
            if connector.mountable
        ]
        directories += [
            Path(volume_config["config"]["path"])
            for volume_name, volume_config in storage_settings.FLOW_VOLUMES.items()
            if volume_config["config"].get("read_only", False) == False
        ]

        for directory in directories:
            directory = directory.resolve()
            for test_dir in directory.iterdir():
                if not test_dir.is_dir():
                    continue
                if not re.match(TEST_DIR_REGEX, test_dir.name):
                    continue

                shutil.rmtree(test_dir)
