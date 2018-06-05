"""Elastic search specialized dependencies for Resolwe."""
from resolwe.elastic.builder import ManyToManyDependency
from resolwe.flow.models import Data


class DoneDataDependency(ManyToManyDependency):
    """Dependency only on DONE data objects."""

    def filter(self, obj, update_fields=None):
        """Determine if object should be processed."""
        return obj.status == Data.STATUS_DONE
