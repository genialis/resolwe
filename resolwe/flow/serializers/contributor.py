"""Resolwe contributor serializer."""
from django.contrib import auth

from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.fields import empty

from resolwe.rest.serializers import SelectiveFieldMixin


class ContributorSerializer(SelectiveFieldMixin, serializers.ModelSerializer):
    """Serializer for contributor User objects."""

    class Meta:
        """Serializer configuration."""

        # The model needs to be determined when instantiating the serializer
        # class as the applications may not yet be ready at this point.
        model = None
        fields = (
            "first_name",
            "id",
            "last_name",
            "username",
        )

    def __init__(self, instance=None, data=empty, **kwargs):
        """Initialize attributes."""
        # Use the correct User model.
        if self.Meta.model is None:
            self.Meta.model = auth.get_user_model()

        super().__init__(instance, data, **kwargs)

    def to_internal_value(self, data):
        """Format the internal value."""
        # When setting the contributor, it may be passed as an integer.
        if isinstance(data, dict) and isinstance(data.get("id", None), int):
            data = data["id"]
        elif isinstance(data, int):
            pass
        else:
            raise ValidationError(
                "Contributor must be an integer or a dictionary with key 'id'"
            )

        return self.Meta.model.objects.get(pk=data)
