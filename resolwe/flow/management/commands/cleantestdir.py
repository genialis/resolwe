""".. Ignore pydocstyle D400.

====================
Clean test directory
====================

Command to run on local machine::

    ./manage.py clean_test_dir

"""
import os
import re
import shutil

from django.core.management.base import BaseCommand

from resolwe.flow.managers.protocol import ExecutorFiles
from resolwe.test.testcases.setting_overrides import FLOW_EXECUTOR_SETTINGS

TEST_DIR_REGEX = r"^test_\d+$"


class Command(BaseCommand):
    """Cleanup files created during testing."""

    help = "Cleanup files created during testing."

    def handle(self, *args, **kwargs):
        """Cleanup files created during testing."""
        for name in ["DATA_DIR", "UPLOAD_DIR", "RUNTIME_DIR"]:
            directory = os.path.abspath(FLOW_EXECUTOR_SETTINGS[name])

            for basename in os.listdir(directory):

                fname = os.path.abspath(os.path.join(directory, basename))

                if not os.path.isdir(fname):
                    continue
                if not re.match(TEST_DIR_REGEX, basename):
                    continue

                def handle_error(func, path, exc_info):
                    """Handle permission errors while removing secrets directory."""
                    if isinstance(exc_info[1], PermissionError):
                        # Verify that such woraround can only be applied to secrets dir.
                        assert os.path.basename(path) == ExecutorFiles.SECRETS_DIR
                        os.chmod(path, 0o700)
                        shutil.rmtree(path)

                shutil.rmtree(fname, onerror=handle_error)
