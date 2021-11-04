""".. Ignore pydocstyle D400.

=======================
Permissions Application
=======================

"""
from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class PermissionsConfig(AppConfig):
    """Permissions AppConfig."""

    name = "resolwe.permissions"
    verbose_name = _("Resolwe Permissions")

    def ready(self):
        """Load anonymous user from database on startup."""
        from resolwe.permissions.models import get_anonymous_user

        get_anonymous_user()
