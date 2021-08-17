""".. Ignore pydocstyle D400.

===================
Update storage type
===================

The command updates data objects storage type based on data retrieved from S3
inventory service.

    ./manage.py update_storage_type inventory_filename

"""
from collections.abc import Iterator
from csv import reader
from datetime import datetime
from pathlib import Path
from typing import Optional, TextIO, Tuple

from psycopg2.extras import DateTimeTZRange

from django.core.management.base import BaseCommand
from django.utils.timezone import now

from resolwe.billing.models import DataHistory, StorageCost, StorageTypeHistory
from resolwe.flow.models import Data


class SubpathStorageTypeIterator:
    """An iterator returning subpath and storage type from inventory file."""

    def __init__(self, inventory_filename: str) -> None:
        """Initialization."""
        self.filename = inventory_filename
        self.handle: Optional[TextIO] = None

    def __del__(self) -> None:
        """Close the file."""
        if self.handle:
            self.handle.close()

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        self.handle = open(self.filename, "rt")
        return self

    def __next__(self) -> Tuple[str, str]:
        """Return the next subpath and storage type."""

        def parse_line(line: str) -> Tuple[str, str]:
            """Parse one line from inventory file.

            TODO: the structure of the inventory file depends on the S3
            inventory configuration. When it changes so must the column
            numbers in the code bellow. Currenty we expect the structure of
            each line in the inventory file to be

            bucket_name,object_name,object_size,hast,storage_type .

            :returns: the tuple (subpath, storage_type).
            """
            data = next(reader([line]))
            filename, storage_type = (data[index] for index in (1, 4))
            return (Path(filename).parts[0], storage_type)

        assert self.handle is not None, "Handler must be initialized in iterator."
        line = self.handle.readline()
        if not line:
            self.handle.close()
            self.handle = None
            raise StopIteration("EOF reached")

        subpath, storage_type = parse_line(line)
        # Objects in the same subpath may have different storage_types. The
        # current solution stores all of them in the set and promotes one
        # at random (using pop method) to be the storage type for the entire
        # data object stored within the subpath.
        storage_types = {storage_type}
        current_subpath = subpath
        current_storage_type = storage_type
        while True:
            current_position = self.handle.tell()
            line = self.handle.readline()
            if not line:
                break
            current_subpath, current_storage_type = parse_line(line)
            if current_subpath == subpath:
                storage_types.add(current_storage_type)
            else:
                self.handle.seek(current_position)
                break

        return subpath, storage_types.pop()


def update_storage_type(
    data_history: DataHistory, new_storage_type: str, current_time: Optional[datetime]
):
    """Update storage type history for one DataHistory instance.

    Assumption: old storage type is valid until now.
    """
    current_time = current_time or now()
    storage_type_history = StorageTypeHistory.objects.filter(
        data_history=data_history, interval__contains=now()
    )
    current_storage_type = storage_type_history.storage_cost.storage_type
    # We only have to change the object when storage type changes.
    # Then we have to set the valid interval on the old StorageHistoryType
    # object and create a new one.
    # The new storage cost will be valid from this moment on.
    if current_storage_type != new_storage_type:
        storage_type_history.interval.upper = current_time
        storage_type_history.save(update_fields=["interval"])
        new_cost = StorageCost.objects.get(storage_type=new_storage_type)
        interval = DateTimeTZRange(lower=current_time)
        StorageTypeHistory.objects.create(
            data_history=data_history, interval=interval, storage_cost=new_cost
        )


class Command(BaseCommand):
    """Cleanup files created during testing."""

    help = "Update storage type based on data from S3 inventory."

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument("inventory_file", help="Name of the inventory file.")

    def handle(self, *args, **options):
        """Command handle."""
        inventory_file = options["inventory_file"]

        current_time = now()
        for subpath, new_storage_type in SubpathStorageTypeIterator(inventory_file):
            data = Data.objects.get(location_id=subpath)
            # Get currently valid data history and storage_type history objects.
            data_history = data.data_history.get(alive__contains=now())
            update_storage_type(data_history, new_storage_type, current_time)
