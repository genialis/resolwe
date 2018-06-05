# Run Cellery with:
# DJANGO_SETTINGS_MODULE='tests.settings' celery worker -A tests --loglevel=info

try:
    from celery import Celery
except ImportError:
    Celery = None

from django.conf import settings


app = None
if Celery:
    app = Celery('resolwe')

    app.config_from_object('django.conf:settings')
    app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
