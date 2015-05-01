"""Utility functions"""


class BraceMessage(object):
    """Helper class that can be used to construct log messages with
    the new {}-string formatting syntax.

    NOTE: When using this helper class, one pays no signigicant
    performance penalty since the actual formatting only happens when
    (and if) the logged message is actually outputted to a log by a
    handler.

    Example usage:
    from genesis.utils.formatters import BraceMessage as __
    logger.error(__("Message with {0} {name}", 2, name="placeholders"))

    Source:
    https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles

    """

    def __init__(self, fmt, *args, **kwargs):
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return self.fmt.format(*self.args, **self.kwargs)
