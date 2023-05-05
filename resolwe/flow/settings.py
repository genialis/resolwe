from django.conf import settings

from resolwe.utils import ApplicationSettings, get_defaults

# The settings that must be present in settings.py.
required_settings = []

missing_required_settings = [
    required_setting
    for required_setting in required_settings
    if not hasattr(settings, required_setting)
]
if missing_required_settings:
    raise RuntimeError(
        f"Settings {missing_required_settings} must be present in Django config."
    )


from . import settings_default

settings = ApplicationSettings(get_defaults(settings_default))
