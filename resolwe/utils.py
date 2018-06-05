""".. Ignore pydocstyle D400.

=================
Resolwe Utilities
=================

"""


class BraceMessage:
    """Log messages with the new {}-string formatting syntax.

    .. note::
        When using this helper class, one pays no significant
        performance penalty since the actual formatting only happens
        when (and if) the logged message is actually outputted to a log
        by a handler.

    Example of usage:

        .. code-block:: python

            from resolwe.utils import BraceMessage as __

            logger.error(__("Message with {0} {name}", 2, name="placeholders"))

    Source:
    https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles.

    """

    def __init__(self, fmt, *args, **kwargs):
        """Initialize attributes."""
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        """Define the object representation."""
        return self.fmt.format(*self.args, **self.kwargs)
