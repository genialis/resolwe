"""Billing estimated cost by collection serializer."""
from rest_framework import serializers

from resolwe.billing.models import (
    EstimatedCostByBillingAccount,
    EstimatedCostByCollection,
)


class EstimatedCostPerCollectionSerializer(serializers.ModelSerializer):
    """Serializer for cost per day per collection."""

    class Meta:
        model = EstimatedCostByCollection
        read_only_fields = (
            "cost_type",
            "date",
            "cost",
            "collection",
            "referenced_collection_slug",
            "referenced_collection_name",
        )
        fields = read_only_fields


class EstimatedCostPerBillingAccountSerializer(serializers.ModelSerializer):
    """Serializer for cost per day per billing account."""

    class Meta:
        model = EstimatedCostByBillingAccount
        read_only_fields = ("cost_type", "date", "cost", "billing_account_id")
        fields = read_only_fields
