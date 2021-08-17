"""Billing viewset by Collections."""
from datetime import datetime

from psycopg2.extras import DateTimeTZRange

from django_filters.rest_framework.backends import DjangoFilterBackend
from rest_framework import mixins, viewsets

from resolwe.billing.exceptions import ValidationError
from resolwe.billing.models import (
    EstimatedCostByBillingAccount,
    EstimatedCostByCollection,
)
from resolwe.billing.serializers import (
    EstimatedCostPerBillingAccountSerializer,
    EstimatedCostPerCollectionSerializer,
)


class EstimatedCostByCollectionViewset(
    mixins.RetrieveModelMixin, viewsets.GenericViewSet, mixins.ListModelMixin
):
    """API view for :class:`EstimatedCostByCollection` objects."""

    queryset = EstimatedCostByCollection.objects.all()
    serializer_class = EstimatedCostPerCollectionSerializer
    filter_backends = (DjangoFilterBackend,)
    filterset_fields = []

    def get_queryset(self):
        """Get filtered queryset based on input parameters.

        The parameters are:
            - date_from: ISO 8601 date representation, required.
            - date_to: ISO 8601 date representation, required.
            - collection_id: string, optional.
            - cost_type: string, optional.

        :raises ValidationError: when dates are not set or are in incorrect
            format.
        """
        date_from = self.request.query_params.get("date_from")
        date_to = self.request.query_params.get("date_to")
        collection_id = self.request.query_params.get("collection_id")
        cost_type = self.request.query_params.get("cost_type")

        if date_from is None or date_to is None:
            raise ValidationError("Parameters date_from and date_to must be set.")

        try:
            lower = datetime.fromisoformat(date_from)
            upper = datetime.fromisoformat(date_to)
        except ValueError:
            raise ValidationError(
                "Parameters date_from and date_to must be valid ISO 8601 dates."
            )

        if upper <= lower:
            raise ValidationError(
                f"Parameter date_from ({lower}) must be strictly smaller than date_to ({upper})."
            )

        queryset = EstimatedCostByCollection.objects.filter(
            date__lte=upper, date__gte=lower
        )

        if collection_id is not None:
            queryset = queryset.filter(collection_id=collection_id)

        if cost_type is not None:
            queryset = queryset.filter(cost_type=cost_type)

        return queryset


class EstimatedCostByBillingAccountViewset(
    mixins.RetrieveModelMixin, viewsets.GenericViewSet, mixins.ListModelMixin
):
    """API view for :class:`EstimatedCostByBillingAccount` objects."""

    queryset = EstimatedCostByBillingAccount.objects.all()
    serializer_class = EstimatedCostPerBillingAccountSerializer
    filter_backends = (DjangoFilterBackend,)
    filterset_fields = []

    def get_queryset(self):
        """Get filtered queryset based on input parameters.

        The parameters are:
            - date_from: ISO 8601 date representation, required.
            - date_to: ISO 8601 date representation, required.
            - billing_account_id: string, optional.
            - cost_type: string, optional.

        :raises ValidationError: when dates are not set or are in incorrect
            format.
        """

        date_from = self.request.query_params.get("date_from")
        date_to = self.request.query_params.get("date_to")
        billing_account_id = self.request.query_params.get("billing_account_id")
        cost_type = self.request.query_params.get("cost_type")

        if date_from is None or date_to is None:
            raise ValidationError("Parameters date_from and date_to must be set.")

        try:
            lower = datetime.fromisoformat(date_from)
            upper = datetime.fromisoformat(date_to)
        except ValueError:
            raise ValidationError(
                "Parameters date_from and date_to must be valid ISO 8601 dates."
            )

        if upper <= lower:
            raise ValidationError(
                f"Parameter date_from ({lower}) must be strictly smaller than date_to ({upper})."
            )

        queryset = EstimatedCostByBillingAccount.objects.filter(
            date__lte=upper, date__gte=lower
        )

        if billing_account_id is not None:
            queryset = queryset.filter(billing_account_id=billing_account_id)

        if cost_type is not None:
            queryset = queryset.filter(cost_type=cost_type)

        return queryset
