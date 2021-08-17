# pylint: disable=missing-docstring
from datetime import datetime
from decimal import Decimal
from time import sleep

from psycopg2.extras import DateTimeTZRange

from django.db import models
from django.utils.timezone import now

from resolwe.billing.cost_estimator import CostEstimator
from resolwe.billing.models import (
    BillingAccount,
    DataHistory,
    EstimatedCostByCollection,
    EstimatedCostByBillingAccount,
)
from resolwe.flow.models import Data
from resolwe.test import TestCase, TransactionTestCase


class BillingStorageTest(TestCase):
    fixtures = [
        "billing_process.yaml",
        "billing_collection.yaml",
        "billing_user.yaml",
        "billing_account.yaml",
        "billing_data.yaml",
        "billing_datahistory.yaml",
        "billing_storage_cost.yaml",
        "billing_storage_type_history.yaml",
        "real_cost.yaml",
    ]

    def test_storage_one_month(self):
        """One month of the Data storage.

        The data object is stil present, only one storage class.
        """
        interval = DateTimeTZRange(lower="2020-01-01", upper="2020-02-01")
        cost_estimator = CostEstimator()
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.9548"))

    def test_storage_half_month_interval(self):
        """Half month of the data storage by interval.

        The data object is stil present, only one storage class. The half-month
        is limited by the interval. The cost is 0.021 gb/month.
        """
        interval = DateTimeTZRange(lower="2020-01-01", upper="2020-01-15")
        cost_estimator = CostEstimator()
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.4312"))

    def test_storage_half_month_history(self):
        """Half month of the data storage by history object.

        The data object is stil present, only one storage class. The half-month
        is limited by the DataHistory object. The cost in 0.01 gb/month.
        """
        interval = DateTimeTZRange(lower="2020-02-01", upper="2020-03-01")
        cost_estimator = CostEstimator()
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.2053"))

    def test_storage_different_interval(self):
        """Storage cost for March 2020.

        Half of the March the cost is 0.01, other half 0.02 gb/month.
        """
        interval = DateTimeTZRange(lower="2020-03-01", upper="2020-04-01")
        cost_estimator = CostEstimator()
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.6893"))

    def test_overlapping(self):
        """Storage cost for April 2020.
        16.-25. April: 0.01
        20.-23. April: 0.02
        """
        # First 3 days, one object.
        cost_estimator = CostEstimator()
        interval = DateTimeTZRange(lower="2020-04-16", upper="2020-04-20")
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.0587"))

        # # First 6 days,
        interval = DateTimeTZRange(lower="2020-04-16", upper="2020-04-23")
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.1907"))

        interval = DateTimeTZRange(lower="2020-04-16", upper="2020-04-25")
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.22"))

        interval = DateTimeTZRange(lower="2020-04-16", upper="2020-04-30")
        storage_queryset = cost_estimator._storage_history_queryset(interval)
        cost = storage_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("0.22"))


class BillingComputeTest(TestCase):
    fixtures = [
        "billing_user.yaml",
        "billing_account.yaml",
        "billing_process.yaml",
        "billing_collection.yaml",
        "billing_data.yaml",
        "billing_datahistory.yaml",
        "billing_computecost.yaml",
        "real_cost.yaml",
    ]

    def test_single_compute(self):
        """One data object compute cost.

        2020-01-01: 00:00 -> 05:00 : 8CPU, 32GB, 0.3328
        2020-01-01: 05:00 -> 10:00 : 4CPU, 16GB, 0.2

        Requirements: 3 CPU, 16G RAM.
        """
        cost_estimator = CostEstimator()
        interval = DateTimeTZRange(
            lower="2020-01-01T00:00:00+00:00", upper="2020-01-01T05:00:00+00:00"
        )
        compute_queryset = cost_estimator._compute_history_queryset(interval)

        # There are two entries: one at share 1/2 and one at share 1/4 for 5 hours.
        # The first one costs 0.832 and the second one 0.416.
        cost = compute_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("1.248"))

        # Nothing should be processing between 5am and 10am.
        interval = DateTimeTZRange(
            lower="2020-01-01T05:00:00+00:00", upper="2020-01-01T10:00:00+00:00"
        )
        compute_queryset = cost_estimator._compute_history_queryset(interval)
        self.assertEqual(compute_queryset.count(), 0)

        # Result should be the same as the first one.
        interval = DateTimeTZRange(
            lower="2020-01-01T00:00:00+00:00", upper="2020-01-01T10:00:00+00:00"
        )
        compute_queryset = cost_estimator._compute_history_queryset(interval)
        cost = compute_queryset.aggregate(total_cost=models.Sum("cost"))["total_cost"]
        self.assertEqual(cost, Decimal("1.2480"))


class BillingSignalsTest(TransactionTestCase):
    """Test signals in the Billing app.

    No DataHistory object should be imported via fixtures: they must be
    created via signals in the Billing app. That is why TransactionTestCase
    is used, otherwise the entire test is wrapped in transaction and
    DataHistory objects are not created.
    """

    fixtures = [
        "billing_process.yaml",
        "billing_collection.yaml",
        "billing_billing_collection.yaml",
        "billing_user.yaml",
        "billing_account.yaml",
        "billing_data.yaml",
        "billing_storage_cost.yaml",
        "real_cost.yaml",
        "billing_computecost.yaml",
    ]

    def test_datahistory_created(self):
        """Make sure DataHistory is created for every Data object."""
        billing_account1 = BillingAccount.objects.get(pk=1)
        billing_account2 = BillingAccount.objects.get(pk=2)
        # Mapping between data and billing account.
        # Data object 3 is not contained in the collection and must have
        # default billing account of the contributor assigned.
        data_billing_account = {
            1: billing_account1,
            2: billing_account1,
            3: billing_account2,
        }
        data_objects = Data.objects.all()
        data_history_objects = DataHistory.objects.all()
        self.assertEqual(data_history_objects.count(), data_objects.count())
        for data in data_objects:
            data_history = data.data_history.get()
            self.assertEqual(data_history.processing.lower, data.started)
            self.assertEqual(data_history.processing.upper, data.finished)
            self.assertIsNone(data_history.deleted)
            self.assertEqual(data_history.collection, data.collection)
            self.assertEqual(data_history.referenced_data_id, data.pk)
            self.assertEqual(data_history.size, data.size)
            self.assertEqual(data_history.cpu, data.process_cores)
            self.assertEqual(data_history.memory, data.process_memory)
            self.assertEqual(data_history.node_cpu, 8)
            self.assertEqual(data_history.node_memory, 32)
            self.assertEqual(
                data_history.billing_account, data_billing_account[data.pk]
            )
            storage_type_history = data_history.storage_type_history.get()
            self.assertEqual(storage_type_history.storage_type, "s3_standard")
            self.assertIsNone(storage_type_history.interval.upper)

    def test_datahistory_nobilling(self):
        """Raise exception if no billing account is found."""

        with self.assertRaisesRegex(
            RuntimeError, r"No billing account found for Data object \d+\."
        ):
            Data.objects.create(
                name="test", slug="test", contributor_id=3, process_id=1
            )

    def test_datahistory_modified(self):
        """Changes to Data must be reflected in DataHistory object."""
        d = Data.objects.get(pk=1)
        data_history = d.data_history.get()
        new_size = d.size + 1
        d.size = new_size
        d.save()
        data_history.refresh_from_db()
        self.assertEqual(data_history.size, new_size)

    def test_data_deleted(self):
        data = Data.objects.get(pk=1)
        data_history = data.data_history.get()
        self.assertIsNone(data_history.deleted)
        sleep(1.1)
        data.delete()
        data_history.refresh_from_db()
        self.assertLess((now() - data_history.deleted).total_seconds(), 1)

    def test_storage_cost_created(self):
        data = Data(
            name="onecost",
            slug="onecost",
            contributor_id=1,
            process_id=1,
            started=datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
            finished=datetime.fromisoformat("2020-02-01T00:00:00+00:00"),
            process_cores=8,
            process_memory=32,
        )
        data.save()
        data_history = data.data_history.get()
        self.assertEqual(data_history.computecosthistory_set.count(), 1)
        compute_cost_history = data_history.computecosthistory_set.get()
        expected_interval = DateTimeTZRange(lower=data.started, upper=data.finished)
        self.assertEqual(compute_cost_history.interval, expected_interval)
        self.assertEqual(compute_cost_history.compute_cost.pk, 1)

        data = Data(
            name="twocost",
            slug="twocost",
            contributor_id=1,
            process_id=1,
            started=datetime.fromisoformat("2020-12-31T23:00:00+00:00"),
            finished=datetime.fromisoformat("2021-01-01T01:00:00+00:00"),
            process_cores=8,
            process_memory=32,
        )
        data.save()

        new_year = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
        data_history = data.data_history.get()
        self.assertEqual(data_history.computecosthistory_set.count(), 2)
        compute_cost_history = data_history.computecosthistory_set.all()
        first = compute_cost_history.get(interval__startswith=data.started)
        second = compute_cost_history.get(interval__startswith=new_year)

        self.assertEqual(first.interval.lower, data.started)
        self.assertEqual(first.interval.upper, new_year)

        self.assertEqual(second.interval.lower, new_year)
        self.assertEqual(second.interval.upper, data.finished)


class SummaryBillingTestclass(TestCase):
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
        "billing_storage_type_history.yaml",
    ]

    def test_compute_january(self):
        cost_estimator = CostEstimator()
        cost_estimator.compute_cost(1, 2020)

        cost_by_collection = EstimatedCostByCollection.objects.all()
        compute_cost_by_collection = cost_by_collection.filter(cost_type="compute")
        self.assertEqual(compute_cost_by_collection.count(), 2)
        # The numbers bellow are exactly the same as in the test
        # test_single_compute above only multiplied with the compute factor.
        self.assertEqual(
            compute_cost_by_collection.get(
                referenced_collection_slug="collection1"
            ).cost,
            Decimal("1.3333"),
        )
        self.assertEqual(
            compute_cost_by_collection.get(
                referenced_collection_slug="collection2"
            ).cost,
            Decimal("0.6667"),
        )

        # There is only storage cost object for first DataHistory object for January.
        storage_cost_by_collection = cost_by_collection.filter(cost_type="storage")
        self.assertEqual(storage_cost_by_collection.count(), 31)
        self.assertEqual(storage_cost_by_collection.first().cost, Decimal("0.1613"))
