"""Elastic Search base index types for Resolwe models."""
import elasticsearch_dsl as dsl

from resolwe.elastic import indices
from resolwe.elastic.fields import Name, Slug, User
from resolwe.permissions.shortcuts import get_users_with_permission
from resolwe.permissions.utils import get_full_perm


class BaseDocument(indices.BaseDocument):
    """Base search document."""

    id = dsl.Integer()  # pylint: disable=invalid-name
    slug = Slug()
    version = dsl.Keyword()
    name = Name()
    created = dsl.Date()
    modified = dsl.Date()
    contributor_id = dsl.Integer()
    contributor_name = User()
    # We use a separate field for contributor sorting because we use an entirely
    # different value for it (the display name).
    contributor_sort = dsl.Keyword()
    owner_ids = dsl.Integer(multi=True)
    owner_names = User(multi=True)


class BaseIndexMixin:
    """Base index for objects used in ``BaseDocument``."""

    def get_contributor_sort_value(self, obj):
        """Generate display name for contributor."""
        user = obj.contributor

        if user.first_name or user.last_name:
            contributor = user.get_full_name()
        else:
            contributor = user.username

        return contributor.strip().lower()

    def _get_user(self, user):
        """Generate user filtering tokens."""
        return ' '.join([user.username, user.first_name, user.last_name])

    def get_contributor_id_value(self, obj):
        """Extract contributor identifier."""
        return obj.contributor.pk

    def get_contributor_name_value(self, obj):
        """Extract contributor name."""
        return self._get_user(obj.contributor)

    def get_owner_ids_value(self, obj):
        """Extract owners' ids."""
        return [
            user.pk
            for user in get_users_with_permission(obj, get_full_perm('owner', obj))
        ]

    def get_owner_names_value(self, obj):
        """Extract owners' names."""
        return [
            self._get_user(user)
            for user in get_users_with_permission(obj, get_full_perm('owner', obj))
        ]

    def get_version_value(self, obj):
        """Extract version metadata."""
        return str(obj.version)
