"""
==================
Flow Configuration
==================

"""

from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class FlowConfig(AppConfig):

    """Flow AppConfig."""

    name = 'resolwe.flow'
    verbose_name = _("Resolwe Dataflow")
