"""Estimate costs for the given time period."""
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Literal

from psycopg2.extras import DateTimeTZRange

from django.db import models

from resolwe.billing.models import (
    DECIMAL_PLACES,
    MAX_DIGITS,
    ActualCost,
    ComputeCost,
    DataHistory,
    EstimatedCostByBillingAccount,
    EstimatedCostByCollection,
    StorageCost,
    StorageTypeHistory,
)


CostType = Literal["storage", "compute"]


class CostEstimator:
    """Estimate cost for the given period."""

    def _annotate_duration(self, queryset: models.QuerySet) -> models.QuerySet:
        """Annotate the given queryset with the duration.

        The queryset must contain field of type interval named ``intersection``.

        The following fields are added to the queryset:
        - a: the start of the interval A
        - b: the end of the interval A
        - duration: the duration field
        - seconds: duration in seconds
        """
        return (
            queryset.annotate(a=models.Func(models.F("intersection"), function="lower"))
            .annotate(b=models.Func(models.F("intersection"), function="upper"))
            .annotate(
                duration=models.functions.Cast(
                    models.F("b") - models.F("a"),
                    output_field=models.DurationField(),
                )
            )
            .annotate(seconds=models.functions.Extract("duration", "epoch"))
        )

    def _storage_history_queryset(
        self, interval: DateTimeTZRange
    ) -> "models.QuerySet[StorageTypeHistory]":
        """Get the queryset of StorageTypeHistory objects.

        Lets name the intersection of the given interval with the given
        StorageTypeHistory object A. Then objects in the returned QuerySet are
        annotated with:
        - cost: the cost for data storage with the given interval, 4 decimal
          places are preserved.

        :returns: QuerySet of StorageTypeHistory objects annotated with ``cost``.
        """
        storage_costs = StorageCost.objects.filter(
            storage_type=models.OuterRef("storage_type"),
            valid__overlap=interval * models.OuterRef("interval"),
        ).annotate(
            intersection=models.F("valid") * interval * models.OuterRef("interval")
        )
        storage_costs = self._annotate_duration(storage_costs)
        storage_costs = storage_costs.annotate(
            partial_cost=models.functions.Cast(
                models.F("seconds")
                / 3600
                / 720
                * models.OuterRef("data_history__size")
                * models.F("cost"),  # 720 hours per month
                output_field=models.DecimalField(max_digits=12, decimal_places=4),
            )
        )

        return StorageTypeHistory.objects.filter(interval__overlap=interval).annotate(
            cost=models.Subquery(
                storage_costs.annotate(
                    storage_history_cost=models.Sum("partial_cost")
                ).values("storage_history_cost")[:1]
            )
        )

    def _compute_history_queryset(
        self, interval: DateTimeTZRange
    ) -> "models.QuerySet[DataHistory]":
        """Get the queryset of annotated DataHistory objects.

        Lets name the intersection of the given interval with the given
        ComputeHistory object A. Then objects in the returned QuerySet are
        annotated with:
        - a: the start of the interval A
        - b: the end of the interval A
        - seconds: the duration of the interval A (in seconds)
        - cost: the cost for data storage with the given interval, 4 decimal
          places are preserved.

        :returns: a DataHistory queryset annotated with ``cost``.
        """
        compute_costs = ComputeCost.objects.filter(
            cpu=models.OuterRef("node_cpu"),
            memory=models.OuterRef("node_memory"),
            valid__overlap=interval * models.OuterRef("processing"),
        ).annotate(
            intersection=models.F("valid") * interval * models.OuterRef("processing")
        )
        compute_costs = self._annotate_duration(compute_costs)
        compute_costs = (
            compute_costs.annotate(
                memory_share=models.functions.Cast(
                    models.OuterRef("memory"),
                    output_field=models.DecimalField(
                        max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES
                    ),
                )
                / models.functions.Cast(
                    models.F("memory"),
                    output_field=models.DecimalField(
                        max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES
                    ),
                )
            )
            .annotate(
                cpu_share=models.functions.Cast(
                    models.OuterRef("cpu"),
                    output_field=models.DecimalField(
                        max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES
                    ),
                )
                / models.functions.Cast(
                    models.F("cpu"),
                    output_field=models.DecimalField(
                        max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES
                    ),
                )
            )
            .annotate(share=models.functions.Greatest("memory_share", "cpu_share"))
            .annotate(
                partial_cost=models.functions.Cast(
                    models.F("seconds")
                    * models.F("cost")  # cost per hour
                    * models.F("share")
                    / 3600,
                    output_field=models.DecimalField(
                        max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES
                    ),
                )
            )
        )

        return DataHistory.objects.filter(processing__overlap=interval).annotate(
            cost=models.Subquery(
                compute_costs.annotate(
                    datahistory_cost=models.Sum("partial_cost")
                ).values("datahistory_cost")[:1]
            )
        )

    def _get_factors(
        self, interval: DateTimeTZRange, estimated_costs: Dict[CostType, Decimal]
    ) -> Dict[CostType, Decimal]:
        """Get the factors connecting estimated cost with real cost.

        :param estimated_cost: mapping between cost type and estimated costs.

        :returns: mapping between cost type and cost factors so that the
            following holds: 'real_cost = estimated_cost * cost_factor'.

        :raises Exception: if real costs are not found for the given interval.
        """
        return {
            cost_type: ActualCost.objects.get(
                interval=interval, cost_type=cost_type
            ).cost
            / estimated_costs[cost_type]
            for cost_type in estimated_costs
        }

    def compute_cost(self, month: int, year: int):
        """Compute cost for the given month.

        :attr month: number between 1 and 12 inclusive (1 = January)
        :attr year: the given year.

        :raises AssertionError: when month in not between 1 and 12.
        """
        assert (
            1 <= month <= 12
        ), f"Month '{month}' must be between 1 and 12 (inclusive)."
        lower = datetime.fromisoformat(f"{year}-{month:02d}-01T00:00:00+00:00")
        upper = datetime.fromisoformat(f"{year}-{(month+1):02d}-01T00:00:00+00:00")
        return self._compute_cost(DateTimeTZRange(lower=lower, upper=upper))

    def _compute_cost(self, interval: DateTimeTZRange):
        """Compute cost by day and store them in the database.

        The interval must start and end at midnight.

        :raises AssertionError: when interval does not start and end at midnight.
        """
        compute_queryset = self._compute_history_queryset(interval)
        storage_queryset = self._storage_history_queryset(interval)

        estimated_total_costs_by_type: Dict[CostType, Decimal] = {
            "compute": compute_queryset.aggregate(total_cost=models.Sum("cost"))[
                "total_cost"
            ],
            "storage": storage_queryset.aggregate(total_cost=models.Sum("cost"))[
                "total_cost"
            ],
        }
        factors = self._get_factors(interval, estimated_total_costs_by_type)

        # Iterate through days and compute costs for each day by Collection and by
        # BillingAccount.
        current_date = interval.lower
        day = timedelta(days=1)
        costs_collection = []
        costs_billing = []

        while current_date < interval.upper:
            current_day = DateTimeTZRange(lower=current_date, upper=current_date + day)
            daily_compute_queryset = self._compute_history_queryset(current_day)
            daily_storage_queryset = self._storage_history_queryset(current_day)

            # print("Current day", current_day)
            # print("Compute", daily_compute_queryset.all())

            # return

            daily_by_collection: Dict[CostType, models.QuerySet] = {
                "compute": daily_compute_queryset.filter(
                    referenced_collection_id__isnull=False
                )
                .values(
                    "referenced_collection_id",
                    "referenced_collection_name",
                    "referenced_collection_slug",
                )
                .annotate(group_cost=models.Sum("cost") * factors["compute"]),
                "storage": daily_storage_queryset.filter(
                    data_history__referenced_collection_id__isnull=False
                )
                .values(
                    referenced_collection_id=models.F(
                        "data_history__referenced_collection_id"
                    ),
                    referenced_collection_name=models.F(
                        "data_history__referenced_collection_name"
                    ),
                    referenced_collection_slug=models.F(
                        "data_history__referenced_collection_slug"
                    ),
                )
                .annotate(group_cost=models.Sum("cost") * factors["storage"]),
            }

            daily_by_billing: Dict[CostType, models.QuerySet] = {
                "compute": daily_compute_queryset.values("billing_account_id").annotate(
                    group_cost=models.Sum("cost") * factors["compute"]
                ),
                "storage": daily_storage_queryset.values(
                    billing_account_id=models.F("data_history__billing_account_id")
                ).annotate(group_cost=models.Sum("cost") * factors["storage"]),
            }

            for cost_type, costs in daily_by_billing.items():
                costs_billing += [
                    EstimatedCostByBillingAccount(
                        cost_type=cost_type,
                        date=current_date,
                        cost=cost["group_cost"],
                        billing_account_id=cost["billing_account_id"],
                    )
                    for cost in costs
                ]
            for cost_type, costs in daily_by_collection.items():
                costs_collection += [
                    EstimatedCostByCollection(
                        cost_type=cost_type,
                        date=current_date,
                        cost=cost["group_cost"],
                        collection_id=cost["referenced_collection_id"],
                        referenced_collection_name=cost["referenced_collection_name"],
                        referenced_collection_slug=cost["referenced_collection_slug"],
                    )
                    for cost in costs
                ]
            current_date += day

        EstimatedCostByCollection.objects.bulk_create(costs_collection)
        EstimatedCostByBillingAccount.objects.bulk_create(costs_billing)