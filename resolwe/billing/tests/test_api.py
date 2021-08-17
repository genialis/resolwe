# pylint: disable=missing-docstring


from django.db.utils import IntegrityError
from psycopg2.errors import ForeignKeyViolation

from rest_framework.test import APIRequestFactory

from resolwe.billing.cost_estimator import CostEstimator
from resolwe.billing.models import BillingAccount, BillingCollection
from resolwe.billing.views import (
    BillingAccountViewSet,
    EstimatedCostByCollectionViewset,
    EstimatedCostByBillingAccountViewset,
)

from resolwe.test import TestCase, TransactionTestCase

factory = APIRequestFactory()


class BillingAccountViewSetCase(TransactionTestCase):
    """Test BillingAccount test case."""

    fixtures = ["billing_user.yaml", "billing_account.yaml", "billing_collection.yaml"]

    def test_fetch(self):
        factory = APIRequestFactory()
        request = factory.get("/billingaccount", {}, format="json")
        view = BillingAccountViewSet.as_view({"get": "list"})
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 3)
        names = {billing_account["customer_name"] for billing_account in data}
        self.assertIn("Billing account 1", names)
        self.assertIn("Billing account 2", names)
        self.assertIn("Billing account 3", names)

    def test_filter_status(self):
        factory = APIRequestFactory()
        request = factory.get("/billingaccount?active=false", {}, format="json")
        view = BillingAccountViewSet.as_view({"get": "list"})
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["customer_name"], "Billing account 3")

        request = factory.get("/billingaccount?active=true", {}, format="json")
        view = BillingAccountViewSet.as_view({"get": "list"})
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 2)
        names = {billing_account["customer_name"] for billing_account in data}
        self.assertIn("Billing account 1", names)
        self.assertIn("Billing account 2", names)

    def test_filter_user(self):
        factory = APIRequestFactory()
        request = factory.get("/billingaccount?user__id=1", {}, format="json")
        view = BillingAccountViewSet.as_view({"get": "list"})
        data = view(request).data
        self.assertEqual(len(data), 2)
        names = {billing_account["customer_name"] for billing_account in data}
        self.assertIn("Billing account 1", names)
        self.assertIn("Billing account 3", names)

    def test_assign_collection(self):
        # Billing account 1 should be associated with collection with id 1.
        account_associations = BillingCollection.objects.filter(billing_account_id=1)
        self.assertEquals(account_associations.count(), 1)
        self.assertEquals(account_associations[0].collection_id, 1)
        associations = BillingCollection.objects.filter(collection_id=2)
        self.assertEquals(associations.count(), 0)

        # Assign collection with id 2 to billing account with id 1.
        factory = APIRequestFactory()
        request = factory.post("/billingaccount", {"collection_id": 2}, format="json")
        view = BillingAccountViewSet.as_view({"post": "collection"})
        data = view(request, pk=1).data
        self.assertIn("collection 2", data)
        self.assertIn("billing account 1", data)
        associations = BillingCollection.objects.filter(collection_id=2)
        self.assertEquals(associations.count(), 1)
        self.assertEquals(associations[0].billing_account_id, 1)
        # Billing account 1 should be associated with collection with id 1 and 2.
        account_associations = BillingCollection.objects.filter(billing_account_id=1)
        self.assertEquals(account_associations.count(), 2)
        self.assertEquals(
            {association.collection_id for association in account_associations},
            set((1, 2)),
        )

        # Reassign collection with id 2 to billing account with id 2.
        request = factory.post("/billingaccount", {"collection_id": 2}, format="json")
        view = BillingAccountViewSet.as_view({"post": "collection"})
        data = view(request, pk=2).data
        self.assertIn("collection 2", data)
        self.assertIn("billing account 2", data)
        associations = BillingCollection.objects.filter(collection_id=2)
        self.assertEquals(associations.count(), 1)
        self.assertEquals(associations[0].billing_account_id, 2)
        # Billing account 1 should be associated with collection with id 1.
        account_associations = BillingCollection.objects.filter(billing_account_id=1)
        self.assertEquals(account_associations.count(), 1)
        self.assertEquals(account_associations[0].collection_id, 1)
        # Billing account 2 should be associated with collection with id 2.
        account_associations = BillingCollection.objects.filter(billing_account_id=2)
        self.assertEquals(account_associations.count(), 1)
        self.assertEquals(account_associations[0].collection_id, 2)

    def test_assign_collection_nonexisting(self):
        """Test assigning collection to non-existing billing account/collection."""
        # Assign non-existing billing account.
        factory = APIRequestFactory()
        request = factory.post("/billingaccount", {"collection_id": 2}, format="json")
        view = BillingAccountViewSet.as_view({"post": "collection"})
        with self.assertRaises(IntegrityError):
            view(request, pk=-1)

        # Assign non-existing collection.
        request = factory.post("/billingaccount", {"collection_id": -1}, format="json")
        view = BillingAccountViewSet.as_view({"post": "collection"})
        with self.assertRaises(IntegrityError):
            view(request, pk=1)


class BillingEstimatedCost(TestCase):
    """Test retrieving billing report by Collection."""

    fixtures = [
        "billing_process.yaml",
        "billing_collection.yaml",
        "billing_billing_collection.yaml",
        "billing_user.yaml",
        "billing_account.yaml",
        "billing_data.yaml",
        "billing_datahistory.yaml",
        "real_cost.yaml",
        "billing_computecost.yaml",
        "billing_storage_cost.yaml",
        "billing_compute_cost_history.yaml",
        "billing_storage_type_history.yaml",
    ]

    def test_basic_billing(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        view = EstimatedCostByBillingAccountViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 11)  # 1 compute + 10 storage objects.
        self.assertEqual(len([d for d in data if d["cost_type"] == "storage"]), 10)
        self.assertEqual(len([d for d in data if d["cost_type"] == "compute"]), 1)

    def test_basic_collection(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)
        view = EstimatedCostByCollectionViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 11)  # 1 compute + 10 storage objects.
        self.assertEqual(len([d for d in data if d["cost_type"] == "storage"]), 10)
        self.assertEqual(len([d for d in data if d["cost_type"] == "compute"]), 1)

    def test_filtering_cost_type_billing(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        view = EstimatedCostByBillingAccountViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&cost_type=compute",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 1)  # 1 compute
        self.assertEqual(data[0]["cost_type"], "compute")

    def test_filtering_billing_account_billing(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        view = EstimatedCostByBillingAccountViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&billing_account_id=1",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 11)  # 10 storage + 1 compute
        self.assertEqual(len([d for d in data if d["cost_type"] == "storage"]), 10)
        self.assertEqual(len([d for d in data if d["cost_type"] == "compute"]), 1)

        view = EstimatedCostByBillingAccountViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&&billing_account_id=-1",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 0)

    def test_filtering_cost_type_collection(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        view = EstimatedCostByCollectionViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&cost_type=compute",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 1)  # 1 compute
        self.assertEqual(data[0]["cost_type"], "compute")

    def test_filtering_collection_account_collection(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        view = EstimatedCostByCollectionViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&collection_id=1",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 11)  # 10 storage + 1 compute
        self.assertEqual(len([d for d in data if d["cost_type"] == "storage"]), 10)
        self.assertEqual(len([d for d in data if d["cost_type"] == "compute"]), 1)

        view = EstimatedCostByCollectionViewset.as_view({"get": "list"})
        request = factory.get(
            "estimatedcostbycollection?date_from=2020-01-01&date_to=2020-01-10&&collection_id=-1",
            {},
            format="json",
        )
        response = view(request)
        data = response.data
        self.assertEqual(len(data), 0)
