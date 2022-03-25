""".. Ignore pydocstyle D400.

=======================
Auditlog Configuration
=======================

"""
from django.apps import AppConfig
from django.utils.translation import gettext_lazy as _


class FlowConfig(AppConfig):
    """Audit log AppConfig."""

    name = "resolwe.auditlog"
    verbose_name = _("Resolwe Auditlog")
