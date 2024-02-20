""".. Ignore pydocstyle D400.

====================
Resolwe Test Helpers
====================

"""

TESTING_CONTEXT = {
    "is_testing": False,
}


def is_testing():
    """Return current testing status."""
    return TESTING_CONTEXT["is_testing"]
