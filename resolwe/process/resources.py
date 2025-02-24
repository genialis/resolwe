"""Resource consumption estimators for processes."""

from collections import defaultdict

PROCESS_RESOURCES = defaultdict(dict)


def estimator(process_slug, resource):
    """Register resource consumption estimator."""

    def decorator(func):
        """Register resource consumption estimator."""
        PROCESS_RESOURCES[process_slug][resource] = func
        return func

    return decorator
