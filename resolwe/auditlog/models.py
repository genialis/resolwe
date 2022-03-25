"""Base model with methods for logging access."""
from typing import List

from django.db import models

from resolwe.auditlog.auditmanager import AccessType, AuditManager


class AuditModel(models.Model):
    """Abstract model that includes access logging methods."""

    class Meta:
        """BaseModel Meta options."""

        abstract = True

    @classmethod
    def from_db(cls, db, field_names: List[str], values: List):
        """Override from_db to register read access."""
        id_index = field_names.index("id")
        id_value = values[id_index]
        AuditManager.register_access(cls, id_value, AccessType.READ, set(field_names))
        return super().from_db(db, field_names, values)

    def save(self, *args, **kwargs):
        """Save the model."""
        access_type = AccessType.CREATE if self.pk is None else AccessType.UPDATE
        AuditManager.register_access(
            self._meta.model, self.pk, access_type, kwargs.get("update_fields")
        )
        super().save(*args, **kwargs)
