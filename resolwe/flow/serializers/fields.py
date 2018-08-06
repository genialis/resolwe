"""Resolwe custom serializer fields."""
from django.core.exceptions import ObjectDoesNotExist
from django.utils.encoding import smart_text

from rest_framework.relations import RelatedField

from resolwe.flow.models import DescriptorSchema
from resolwe.permissions.shortcuts import get_objects_for_user
from resolwe.permissions.utils import get_full_perm

from .descriptor import DescriptorSchemaSerializer


class ResolweSlugRelatedField(RelatedField):
    """
    Resolwe specific implementation of SlugRelatedField.

    A read-write field that represents the target of the relationship
    by a unique combination of 'slug' and 'version' attributes.

    This is a modification of rest_framework.relations.SlugRelatedField.
    Since slug is not unique, (but combination of slug and version is),
    we filter objects by slug, by permissions and return object with
    highest version.

    """

    default_error_messages = {
        'does_not_exist': ('Invalid {model_name} {slug_name} "{value}" - object does not exist.'),
        'invalid': ('Invalid value.'),
    }

    def __init__(self, slug_field='slug', **kwargs):
        """Initialize attributes."""
        self.slug_field = slug_field
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        """Convert to internal value."""
        user = getattr(self.context.get('request'), 'user')
        queryset = self.get_queryset()
        permission = get_full_perm('view', queryset.model)
        try:
            return get_objects_for_user(
                user,
                permission,
                queryset.filter(**{self.slug_field: data}),
            ).latest()
        except ObjectDoesNotExist:
            self.fail(
                'does_not_exist',
                slug_name=self.slug_field,
                value=smart_text(data),
                model_name=queryset.model._meta.model_name,  # pylint: disable=protected-access
            )
        except (TypeError, ValueError):
            self.fail('invalid')

    def to_representation(self, obj):
        """Convert to representation."""
        return obj.pk


class NestedDescriptorSchemaSerializer(ResolweSlugRelatedField):
    """DescriptorSchema specific implementation of ResolweSlugRelatedField."""

    def __init__(self, **kwargs):
        """Initialize attributes."""
        kwargs['queryset'] = DescriptorSchema.objects.all()
        super().__init__(slug_field='slug', **kwargs)

    def to_representation(self, obj):
        """Convert to representation."""
        return DescriptorSchemaSerializer(obj, required=self.required).data
