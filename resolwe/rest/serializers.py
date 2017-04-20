"""Support for selective field serialization."""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy

from .projection import apply_subfield_projection


class SelectiveFieldMixin(object):
    """Mixin that enables field selection based on request arguments."""

    @property
    def fields(self):
        """Filter fields based on request query parameters."""
        fields = super(SelectiveFieldMixin, self).fields
        return apply_subfield_projection(self, copy.copy(fields))
