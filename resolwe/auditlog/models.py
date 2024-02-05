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
        field_values = dict(zip(field_names, values))
        id_value = -1
        id_field_names = ["id", "uuid", "user_id"]
        for id_field_name in id_field_names:
            if id_field_name in field_values:
                id_value = field_values[id_field_name]
                break
        AuditManager.register_access(cls, id_value, AccessType.READ, set(field_names))
        return super().from_db(db, field_names, values)

    def save(self, *args, **kwargs):
        """Save the model."""
        access_type = AccessType.CREATE if self.pk is None else AccessType.UPDATE
        AuditManager.register_access(
            self._meta.model, self.pk, access_type, kwargs.get("update_fields")
        )
        super().save(*args, **kwargs)
