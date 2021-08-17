"""Billing application configuration."""
import logging
from contextlib import suppress
from importlib import import_module

from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _

from resolwe.storage.connectors import connectors
from resolwe.storage.connectors.baseconnector import BaseStorageConnector
from resolwe.storage.settings import STORAGE_CONNECTORS

logger = logging.getLogger(__name__)


class BillingConfig(AppConfig):
    """Billing application configuration."""

    name = "resolwe.billing"
    verbose_name = _("Resolwe Billing")

    def ready(self):
        """Application initialization."""
        # Register signals handlers
        from . import signals  # noqa: F401
