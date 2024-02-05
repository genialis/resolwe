"""Decorators for observers."""

from contextlib import suppress

from resolwe.observers.models import Observable
from resolwe.observers.protocol import (
    post_container_changed,
    pre_container_changed,
    suppress_notifications_attribute,
)


def move_to_container(function):
    """Indicate object is moving between containers.

    The notifications are suppressed for the first argument (usually self) of the given
    function which must be the observable object.
    """

    def wrapper(self, *args, **kwargs):
        pre_container_changed.send(sender=type(self), instance=self)
        suppress_observer_notifications(function)(self, *args, **kwargs)
        post_container_changed.send(sender=type(self), instance=self)

    return wrapper


def suppress_observer_notifications(function):
    """Suppress the observer notifications.

    The notifications are suppressed for the first argument (usually self) of the given
    function which must be the observable object.

    :raises AssertionError: when first argument of the given function is not observable
        object.
    """

    def wrapper(self, *args, **kwargs):
        assert isinstance(
            self, Observable
        ), "The first argument of the method must be observable object."
        setattr(self, suppress_notifications_attribute, True)
        try:
            function(self, *args, **kwargs)
        finally:
            with suppress(AttributeError):
                delattr(self, suppress_notifications_attribute)

    return wrapper
