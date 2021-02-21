"""Resolwe secrets model."""
import uuid

from fernet_fields import EncryptedTextField

from django.conf import settings
from django.db import models


class SecretManager(models.Manager):
    """Manager for Secret objects."""

    def create_secret(self, value, contributor, metadata=None, expires=None):
        """Create a new secret, returning its handle.

        :param value: Secret value to store
        :param contributor: User owning the secret
        :param metadata: Optional metadata dictionary (must be JSON serializable)
        :param expires: Optional date/time of expiry (defaults to None, which means that
            the secret never expires)
        :return: Secret handle
        """
        if metadata is None:
            metadata = {}

        secret = self.create(
            value=value,
            contributor=contributor,
            metadata=metadata,
            expires=expires,
        )
        return str(secret.handle)

    def get_secret(self, handle, contributor):
        """Retrieve an existing secret's value.

        :param handle: Secret handle
        :param contributor: User instance to perform contributor validation,
            which means that only secrets for the given contributor will be
            looked up.
        """
        queryset = self.all()
        if contributor is not None:
            queryset = queryset.filter(contributor=contributor)
        secret = queryset.get(handle=handle)
        return secret.value


class Secret(models.Model):
    """Postgres model for storing secrets."""

    #: creation date and time
    created = models.DateTimeField(auto_now_add=True, db_index=True)

    #: modified date and time
    modified = models.DateTimeField(auto_now=True, db_index=True)

    #: user that created the secret
    contributor = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT)

    #: secret handle
    handle = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    #: actual secret
    value = EncryptedTextField()

    #: secret metadata (not encrypted)
    metadata = models.JSONField(default=dict)

    #: expiry time
    expires = models.DateTimeField(null=True, db_index=True)

    #: secret manager
    objects = SecretManager()
