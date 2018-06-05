"""Support for selective field serialization."""
from rest_framework.fields import JSONField

from .projection import apply_subfield_projection


class ProjectableJSONField(JSONField):
    """JSON field which supports projection."""

    def to_representation(self, value):
        """Project outgoing native value."""
        value = apply_subfield_projection(self, value, deep=True)
        return super().to_representation(value)
