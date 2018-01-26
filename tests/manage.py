#!/usr/bin/env python
import channels.log
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, PROJECT_ROOT)


def get_channels_logger(*args, **kwargs):
    """Return logger for channels."""
    return logging.getLogger("django.channels")


# Force channels to respect logging configurations from settings:
# https://github.com/django/channels/issues/520
channels.log.setup_logger = get_channels_logger

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.settings")

    # if len(sys.argv) > 1 and sys.argv[1] == 'runserver':
    #     raise ValueError('This Django project is not intended for running a server.')

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
