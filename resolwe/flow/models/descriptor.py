"""Resolwe descriptor schema model."""
from django.db import models

from .base import BaseModel


class DescriptorSchema(BaseModel):
    """Postgres model for storing descriptors."""

    class Meta(BaseModel.Meta):
        """DescriptorSchema Meta options."""

        permissions = (
            ("view_descriptorschema", "Can view descriptor schema"),
            ("edit_descriptorschema", "Can edit descriptor schema"),
            ("share_descriptorschema", "Can share descriptor schema"),
            ("owner_descriptorschema", "Is owner of the description schema"),
        )

    #: detailed description
    description = models.TextField(blank=True)

    #: user descriptor schema represented as a JSON object
    schema = models.JSONField(default=list)
