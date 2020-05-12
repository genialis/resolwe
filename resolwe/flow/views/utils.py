"""Resolwe views utils."""
from rest_framework import exceptions

from resolwe.flow.models import Collection


def get_collection_for_user(collection_id, user):
    """Check that collection exists and user has `edit` permission."""
    collection_query = Collection.objects.filter(pk=collection_id)
    if not collection_query.exists():
        raise exceptions.ValidationError("Collection id does not exist")

    collection = collection_query.first()
    if not user.has_perm("edit_collection", obj=collection):
        if user.is_authenticated:
            raise exceptions.PermissionDenied()
        else:
            raise exceptions.NotFound()

    return collection
