"""Resolwe custom serializer fields."""
from django.core.exceptions import ObjectDoesNotExist
from django.utils.encoding import smart_text

from rest_framework.relations import RelatedField

from resolwe.flow.models import DescriptorSchema
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import get_full_perm

from .descriptor import DescriptorSchemaSerializer


class DictRelatedField(RelatedField):
    """
    Field representing the target of the relationship by using dict.

    A read-write field that represents the target of the relationship by
    a dictionary, where objects are uniquely defined by ``id`` or
    ``slug`` keys in the dictionary. If ``id`` is provided the uniquness
    of object is arbitrary. In ``id`` is not in the dict, object is
    determined by slug. Since multiple objects can have same slug, we
    filter objects by slug, by permissions and return object with
    highest version.
    """

    default_error_messages = {
        'does_not_exist': ('Invalid {model_name} value: {value} - object does not exist.'),
        'no_data': ('Neither id nor slug is given for field {name}.'),
    }

    def to_representation(self, obj):
        """Convert to representation."""
        return {
            'id': obj.id,
            'slug': obj.slug,
        }

    def to_internal_value(self, data):
        """Convert to internal value."""
        user = getattr(self.context.get('request'), 'user')
        queryset = self.get_queryset()
        permission = get_full_perm('view', queryset.model)

        if data.get('id', None) is not None:
            kwargs = {'id': data['id']}
        elif data.get('slug', None) is not None:
            kwargs = {'slug': data['slug']}
        else:
            self.fail('no_data', name=self.field_name)

        try:
            return get_objects_for_user(
                user,
                permission,
                queryset.filter(**kwargs),
            ).latest()
        except ObjectDoesNotExist:
            self.fail(
                'does_not_exist',
                value=smart_text(data),
                model_name=queryset.model._meta.model_name,  # pylint: disable=protected-access
            )


class NestedDescriptorSchemaSerializer(DictRelatedField):
    """DescriptorSchema specific implementation of DictRelatedField."""

    def __init__(self, **kwargs):
        """Initialize attributes."""
        kwargs['queryset'] = DescriptorSchema.objects.all()
        super().__init__(**kwargs)

    def to_representation(self, obj):
        """Convert to representation."""
        return DescriptorSchemaSerializer(obj, required=self.required).data
