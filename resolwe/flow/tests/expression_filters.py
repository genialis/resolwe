""".. Ignore pydocstyle D400.

==================
Test template tags
==================

"""


def increase(value):
    """Test template tag that returns an increased value."""
    return value + 1


# A dictionary of filters that will be registered.
filters = {  # pylint: disable=invalid-name
    'increase': increase,
}
