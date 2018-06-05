"""Support for selective field serialization."""
import copy

from .projection import apply_subfield_projection


class SelectiveFieldMixin:
    """Mixin that enables field selection based on request arguments."""

    @property
    def fields(self):
        """Filter fields based on request query parameters."""
        fields = super().fields
        return apply_subfield_projection(self, copy.copy(fields))
