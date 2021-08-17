"""Billing viewset by Collections."""
from django_filters.rest_framework.backends import DjangoFilterBackend
from rest_framework import mixins, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.models import Collection
from resolwe.billing.exceptions import ValidationError
from resolwe.billing.models import BillingAccount, BillingCollection
from resolwe.billing.serializers import BillingAccountSerializer


class BillingAccountViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    """API view for :class:`BillingAccounts` objects."""

    queryset = BillingAccount.all_objects.all()
    serializer_class = BillingAccountSerializer
    filterset_fields = ["user__id", "active"]
    filter_backends = (DjangoFilterBackend,)

    @action(detail=True, methods=["post"])
    def collection(self, request, pk=None):
        """Assign collection to the billing account.

        Required arguments: collection_id.
        """
        collection_id = self.request.data["collection_id"]

        # Read previous association of the given Collection with existing Billing account.
        existing = BillingCollection.objects.filter(collection_id=collection_id)

        if existing.count() > 1:
            raise ValidationError(
                f"Collection with id {collection_id} is owned by more than one billing account."
            )
        elif existing.count() == 1:
            existing_association = existing.get()
            if existing_association.billing_account_id != pk:
                existing_association.billing_account_id = pk
                existing_association.save(update_fields=["billing_account_id"])
        else:
            BillingCollection(collection_id=collection_id, billing_account_id=pk).save()

        return Response(f"Assigned collection {collection_id} to billing account {pk}.")
