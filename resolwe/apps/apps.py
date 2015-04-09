from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class AppsConfig(AppConfig):

    """Apps AppConfig."""

    name = 'resolwe.apps'
    verbose_name = _("Resolwe Apps and Packages")
