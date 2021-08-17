"""Billing estimated cost by collection serializer."""
from rest_framework import serializers

from resolwe.billing.models import BillingAccount


class BillingAccountSerializer(serializers.ModelSerializer):
    """Billing account serializer."""

    # collection_id = Field(source='get_absolute_url')

    class Meta:
        model = BillingAccount
        read_only_fields = (
            "user_id",
            "active",
            "customer_name",
        )
        fields = read_only_fields
