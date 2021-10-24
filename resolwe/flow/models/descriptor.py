"""Resolwe descriptor schema model."""
from django.db import models

from resolwe.permissions.models import PermissionObject

from .base import BaseModel


class DescriptorSchema(BaseModel, PermissionObject):
    """Postgres model for storing descriptors."""

    class Meta(BaseModel.Meta):
        """DescriptorSchema Meta options."""

        permissions = (
            ("view", "Can view descriptor schema"),
            ("edit", "Can edit descriptor schema"),
            ("share", "Can share descriptor schema"),
            ("owner", "Is owner of the description schema"),
        )

    #: detailed description
    description = models.TextField(blank=True)

    #: user descriptor schema represented as a JSON object
    schema = models.JSONField(default=list)
