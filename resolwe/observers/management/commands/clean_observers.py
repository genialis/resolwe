""".. Ignore pydocstyle D400.

=======================
Clean old observer data
=======================

Command to run:

    ./manage.py clean_observers

"""

import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

from resolwe.observers.models import Observer, Subscription

# Delete subscriptions older than DELETE_OLDER_THAN hours.
DELETE_OLDER_THAN = 5 * 24

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Delete old observers subscriptions."""

    help = "Delete old observer subscriptions."

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument(
            "--older-than",
            type=int,
            dest="older_than",
            help="Delete subscriptions older than older-than hours",
        )

    def handle(self, *args, **options):
        """Delete old observer subscriptions."""
        delete_older_than = options["older_than"] or DELETE_OLDER_THAN
        logger.info(
            "Deleting observer subscriptions older than %d hours.", delete_older_than
        )
        # Delete subscriptions older than delete_older_than hours.
        deleted_subscriptions = Subscription.objects.filter(
            created__lt=timezone.now() - timezone.timedelta(hours=delete_older_than)
        ).delete()[0]
        logger.info("Deleted %d subscriptions.", deleted_subscriptions)
        # Delete observers without subscriptions.
        deleted_observers = Observer.objects.filter(
            subscriptions__isnull=True
        ).delete()[0]
        logger.info("Deleted %d observers.", deleted_observers)
