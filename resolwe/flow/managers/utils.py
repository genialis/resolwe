""".. Ignore pydocstyle D400.

=========
Utilities
=========

Utilities for using global manager features.

"""

from django.test import override_settings


def disable_auto_calls():
    """Decorator/context manager which stops automatic manager calls.

    When entered, automatic
    :meth:`~resolwe.flow.managers.dispatcher.Manager.communicate` calls
    from the Django transaction signal are not done.
    """
    return override_settings(FLOW_MANAGER_DISABLE_AUTO_CALLS=True)
