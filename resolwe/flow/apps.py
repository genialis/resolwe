"""
==================
Flow Configuration
==================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class FlowConfig(AppConfig):

    """Flow AppConfig."""

    name = 'resolwe.flow'
    verbose_name = _("Resolwe Dataflow")

    def ready(self):
        """
        Performs application initialization.
        """

        # Register signals handlers
        from . import signals  # pylint: disable=unused-import
