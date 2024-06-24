"""Resolwe custom serializer fields."""

from collections import OrderedDict

from django.core.exceptions import ObjectDoesNotExist
from django.utils.encoding import smart_str
from drf_spectacular.utils import extend_schema_field
from rest_framework import exceptions, relations, serializers

from resolwe.permissions.models import Permission


class PrimaryKeyDictRelatedField(serializers.PrimaryKeyRelatedField):
    """Deserialize/serialize model to the dict with 'id' key."""

    def to_internal_value(self, data):
        """Extract id from dict and use the parent method."""
        try:
            if not isinstance(data, dict) or "id" not in data:
                raise TypeError
            return super().to_internal_value(data["id"])
        except (TypeError, ValueError):
            self.fail("incorrect_type", data_type=type(data).__name__)

    def to_representation(self, value):
        """Convert to dict representation - only id gets serialized."""
        return {"id": value.pk}


class RelationshipDictSerializer(serializers.Serializer):
    """Serializer for the DictRelatedField relation."""

    id = serializers.IntegerField(required=False)
    slug = serializers.CharField(required=False, allow_null=True)


@extend_schema_field(RelationshipDictSerializer)
class DictRelatedField(relations.RelatedField):
    """
    Field representing the target of the relationship by using dict.

    A read-write field that represents the target of the relationship by
    a dictionary, where objects are uniquely defined by ``id`` or
    ``slug`` keys in the dictionary. If ``id`` is provided the uniquness
    of object is arbitrary. If ``id`` is not in the dict, object is
    determined by slug. Since multiple objects can have same slug, we
    filter objects by slug, by permissions and return object with
    highest version.
    """

    default_error_messages = {
        "slug_not_allowed": ("Use id (instead of slug) for update requests."),
        "null": ("At least one of id, slug must be present in field {name}."),
        "does_not_exist": (
            "Invalid {model_name} value: {value} - object does not exist."
        ),
        "permission_denied": (
            "You do not have {permission} permission for {model_name}: {value}."
        ),
    }

    def __init__(self, serializer, write_permission=Permission.VIEW, **kwargs):
        """Initialize attributes."""
        self.serializer = serializer
        self.write_permission = write_permission
        super().__init__(**kwargs)

    @property
    def model_name(self):
        """Get name of queryset model."""
        return self.get_queryset().model._meta.model_name

    def to_internal_value(self, data):
        """Convert to internal value."""
        # Allow None values only in case field is not required.
        if "id" in data and data["id"] is None and not self.required:
            return
        elif data.get("id", None) is not None:
            kwargs = {"id": data["id"]}
        elif data.get("slug", None) is not None:
            if self.root.instance:
                # ``self.root.instance != None`` means that an instance is
                # already present, so this is not "create" request.
                self.fail("slug_not_allowed")
            kwargs = {"slug": data["slug"]}
        else:
            self.fail("null", name=self.field_name)

        user = getattr(self.context.get("request"), "user")
        queryset = self.get_queryset()
        permission = self.write_permission
        try:
            return (
                queryset.filter(**kwargs)
                .filter_for_user(user, permission)
                .latest("version")
            )
        except ObjectDoesNotExist:
            # Differentiate between "user has no permission" and "object does not exist"
            view_permission = Permission.VIEW
            if permission != view_permission:
                try:
                    queryset.filter(**kwargs).filter_for_user(
                        user, view_permission
                    ).latest("version")
                    raise exceptions.PermissionDenied(
                        "You do not have {} permission for {}: {}.".format(
                            self.write_permission, self.model_name, data
                        )
                    )
                except ObjectDoesNotExist:
                    pass

            self.fail(
                "does_not_exist", value=smart_str(data), model_name=self.model_name
            )

    def to_representation(self, obj):
        """Convert to representation."""
        serializer = self.serializer(obj, required=self.required, context=self.context)

        # Manually bind this serializer to field.parent as field projection
        # relies on parent/child relations between serializers.
        serializer.bind(self.field_name, self.parent)

        return serializer.data

    def get_choices(self, cutoff=None):
        """
        Get choices for a field.

        The caller of this method is BrowsableAPIRenderer (indirectly).

        Method is direct copy of ``RelatedField.get_choices``, with one
        modified line (noted below). Line is modified so ``OrderedDict``
        receives hashable keys. The impementation in parent class
        returns result of ``self.to_representaton``, which is not
        hashable in case of this field (``self.to_representaton``
        returns a ``ReturnDict`` object).
        """
        queryset = self.get_queryset()
        if queryset is None:
            # Ensure that field.choices returns something sensible
            # even when accessed with a read-only field.
            return {}

        if cutoff is not None:
            queryset = queryset[:cutoff]

        return OrderedDict(
            [
                (
                    # This line below is modifed.
                    item.pk,
                    self.display_value(item),
                )
                for item in queryset
            ]
        )
