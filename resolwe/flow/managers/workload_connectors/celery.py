""".. Ignore pydocstyle D400.

================
Celery Connector
================

"""
import logging
import sys

from django.conf import settings

from resolwe.flow.models import Process
from resolwe.flow.tasks import celery_run
from resolwe.utils import BraceMessage as __

from .base import BaseConnector

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

# Sphinx directly imports the modules it's documenting, so we need to
# guard from importing celery on installations which are configured to
# not use celery and thus don't have it available.
if 'sphinx' not in sys.modules:
    try:
        import celery  # pylint: disable=unused-import
    except ImportError:
        logger.error("Please install Celery using 'pip install celery'", file=sys.stderr)
        sys.exit(1)


class Connector(BaseConnector):
    """Celery-based connector for job execution."""

    def submit(self, data, runtime_dir, argv):
        """Run process.

        For details, see
        :meth:`~resolwe.flow.managers.workload_connectors.base.BaseConnector.submit`.
        """
        queue = 'ordinary'
        if data.process.scheduling_class == Process.SCHEDULING_CLASS_INTERACTIVE:
            queue = 'hipri'

        logger.debug(__(
            "Connector '{}' running for Data with id {} ({}) in celery queue {}, EAGER is {}.",
            self.__class__.__module__,
            data.id,
            repr(argv),
            queue,
            getattr(settings, 'CELERY_ALWAYS_EAGER', None)
        ))
        celery_run.apply_async((data.id, runtime_dir, argv), queue=queue)
