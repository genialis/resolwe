""".. Ignore pydocstyle D400.

======================
Permissions Management
======================

.. automodule:: resolwe.flow.management.commands.register
    :members:

"""


from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import signals


def create_anonymous_user(sender, **kwargs):
    """Create anonymous User instance if it does not exist."""
    User = get_user_model()
    try:
        User.objects.get(username=settings.ANONYMOUS_USER_NAME)
    except User.DoesNotExist:
        User.objects.create_user(username=settings.ANONYMOUS_USER_NAME)


# Create Anonymous user on post migrate signal. This includes "flush" which is
# performed it tests originating from TransactionTestCase.
signals.post_migrate.connect(create_anonymous_user)
