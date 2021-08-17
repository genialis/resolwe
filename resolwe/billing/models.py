"""Resolwe billing model."""
import logging

from django.db import models
from django.conf import settings
from django.contrib.postgres.fields import DateTimeRangeField, RangeOperators
from django.contrib.postgres.constraints import ExclusionConstraint

from resolwe.flow.models import Collection, Data

logger = logging.getLogger(__name__)

# How many digits to use to store computed cost.
# Settings support decimal numbers up to 99_999_999 with 4 digit precision.
MAX_DIGITS = 12
# How many of these digits are reserved for decimal places.
DECIMAL_PLACES = 4


class ActiveBillingAccountManager(models.Manager):
    """Return only active BillingAccount objects."""

    def get_queryset(self) -> models.QuerySet:
        """Override default queryset."""
        return super().get_queryset().filter(active=True)


class AllBillingAccountsManager(models.Manager):
    """Return all (even inactive) BillingAccount objects."""


class BillingAccount(models.Model):
    """The costs are computed per BillingAccount."""

    def delete(self, *args, **kwargs):
        """Set the active flag to false.

        To preserve the history BillingAccount should never be deleted.
        """
        self.active = False
        self.save()

    # access all objects through all_objects storage manager.
    all_objects = AllBillingAccountsManager()

    # by default return only active BillingAccounts
    objects = ActiveBillingAccountManager()

    #: the user for this billing account
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="billing_account",
    )

    #: when deleting BillingAccount active is set to False
    active = models.BooleanField(default=True)

    #: name of the customer (used for identification)
    customer_name = models.CharField(max_length=128)


class DefaultUserBillingAccount(models.Model):
    """Default BillingAccount for the user."""

    #: reference to the User model
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="default_user_billing",
    )

    #: reference to the BillingAccount object
    billing_account = models.OneToOneField(BillingAccount, on_delete=models.CASCADE)


class BillingCollection(models.Model):
    """Connects Collection with the BillingAccount."""

    #: reference to a Collection
    collection = models.OneToOneField(
        Collection, on_delete=models.CASCADE, related_name="billing_collection"
    )

    #: reference to a BillingAccount
    billing_account = models.ForeignKey(BillingAccount, on_delete=models.CASCADE)


class DataHistory(models.Model):
    """Keeps track of Data instance resource usage even after it is deleted."""

    #: reference to a Data instance
    data = models.ForeignKey(
        Data, on_delete=models.SET_NULL, null=True, related_name="data_history"
    )

    #: interval when this DataHistory object is alive
    alive = DateTimeRangeField()

    #: processing interval for this DataHistory object.
    processing = DateTimeRangeField(blank=True, null=True)

    #: deletion date and time
    deleted = models.DateTimeField(db_index=True, null=True, blank=True)

    #: reference to a Collection instance
    collection = models.ForeignKey(Collection, null=True, on_delete=models.SET_NULL)

    #: reference to a Collection slug
    referenced_collection_slug = models.CharField(max_length=100, null=True)

    #: reference to a Collection name
    referenced_collection_name = models.CharField(max_length=100, null=True)

    #: reference to a Collection id
    referenced_collection_id = models.PositiveIntegerField(null=True)

    #: how many cpu cores were requested during computation
    cpu = models.PositiveSmallIntegerField()

    #: number of CPUs on allocated node
    node_cpu = models.PositiveSmallIntegerField()

    #: how many gigabytes of memory were requested during computation
    memory = models.PositiveSmallIntegerField()

    #: number of memory (in gigabytes) on allocated node
    node_memory = models.PositiveSmallIntegerField()

    #: id of the referenced Data instance
    referenced_data_id = models.PositiveIntegerField()

    #: reference to BillingAccount instance
    billing_account = models.ForeignKey(BillingAccount, on_delete=models.PROTECT)

    #: size of the data object (in GB)
    size = models.BigIntegerField()


class Cost(models.Model):
    """Common fields grouped together."""

    #: cost per unit
    cost = models.DecimalField(max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES)

    #: datetime interval when cost is valid (can be unbounded from above)
    valid = DateTimeRangeField()

    class Meta:
        abstract = True


class StorageCost(Cost):
    """Storage cost per gigabyte per month."""

    #: name of the storage type
    storage_type = models.CharField(max_length=64)

    class Meta:
        constraints = [
            ExclusionConstraint(
                name="exclude_overlapping_price_ranges",
                expressions=[
                    ("valid", RangeOperators.OVERLAPS),
                    ("storage_type", RangeOperators.EQUAL),
                ],
            ),
        ]


class ComputeCost(Cost):
    """Cost of the computation."""

    #: number of CPUs available on the node.
    cpu = models.PositiveSmallIntegerField()

    #: ammount of RAM available on the node.
    memory = models.PositiveIntegerField()

    class Meta:
        constraints = [
            ExclusionConstraint(
                name="exclude_overlapping_prices",
                expressions=[
                    ("valid", RangeOperators.OVERLAPS),
                    ("cpu", RangeOperators.EQUAL),
                    ("memory", RangeOperators.EQUAL),
                ],
            ),
        ]


class StorageTypeHistory(models.Model):
    """Information about storage type changes.

    This relation has to be updated periodically to reflect latest changes.
    """

    #: reference to DataHistory instance
    data_history = models.ForeignKey(
        DataHistory, on_delete=models.CASCADE, related_name="storage_type_history"
    )

    #: the interval when Data was stored with the given StorageCost
    interval = DateTimeRangeField()

    #: reference to StorageCost instance
    storage_type = models.CharField(max_length=64)


class EstimatedCost(models.Model):
    """Estimated cost for one day.

    This is abstract class used to group common fields in other models.
    """

    #: cost type (storage/computational/network/...)
    cost_type = models.CharField(max_length=32)

    #: the date for which cost is computed
    date = models.DateField()

    #: estimated cost
    cost = models.DecimalField(max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES)

    class Meta:
        abstract = True


class EstimatedCostByCollection(EstimatedCost):
    """Estimated cost for the given time period."""

    #: reference to Collection instance
    collection = models.ForeignKey(Collection, null=True, on_delete=models.SET_NULL)

    #: reference to the collection name, important if collection is deleted
    referenced_collection_name = models.CharField(max_length=100)

    #: reference to the collection slug, important if collection is deleted
    referenced_collection_slug = models.CharField(max_length=100)

    def __str__(self):
        """String representation of an instance."""
        return (
            f"EstimatedCostByCollection({self.cost_type}, {self.date}, "
            f"{self.cost}, {self.collection}, {self.referenced_collection_name}, "
            f"{self.referenced_collection_slug})"
        )


class EstimatedCostByBillingAccount(EstimatedCost):
    """Estimated cost for the given time period.

    The cost also includes the data objects not included in any collection.
    """

    #: reference to the BillingAccount instance
    billing_account = models.ForeignKey(BillingAccount, on_delete=models.PROTECT)

    def __str__(self):
        """"""
        return (
            f"EstimatedCostByBillingAccount({self.cost_type}, {self.date}, "
            f"{self.cost}, {self.billing_account})"
        )


class ActualCost(models.Model):
    """Actual platform cost for the given period."""

    #: the given interval
    interval = DateTimeRangeField()

    #: reported cost (up to 99_999_999 with 2 digit precision)
    cost = models.DecimalField(max_digits=MAX_DIGITS, decimal_places=DECIMAL_PLACES)

    #: cost type (storage/computational/network/...)
    cost_type = models.CharField(max_length=32)

    def __str__(self) -> str:
        """String representation."""
        return f"ActualCost({self.interval}, {self.cost}, {self.cost_type})"
