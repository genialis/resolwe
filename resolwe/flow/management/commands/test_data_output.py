""".. Ignore pydocstyle D400.

==============================================================
Test if required file outputs of completed Data objects exists
==============================================================

Command to run on local machine::

    ./manage.py test_data_output

"""
from pathlib import Path
from typing import Set

from django.core.management.base import BaseCommand

from resolwe.flow.models import Data
from resolwe.flow.utils import iterate_fields


class Command(BaseCommand):
    """Test if file outputs of completed Data objects exists."""

    help = "Test if required file outputs of completed Data objects exists."

    def get_files_dirs(self, output: dict, output_schema: dict) -> Set[Path]:
        """Get a set of required files and dirs for a given output."""
        paths_to_check: Set[Path] = set()
        for field_schema, fields in iterate_fields(output, output_schema):
            name = field_schema["name"]
            value = fields[name]
            if "type" in field_schema and field_schema.get("required", False):
                if field_schema["type"].startswith("basic:file:"):
                    paths_to_check.add(Path(value["file"]))
                    # paths_to_check.update(Path(ref) for ref in value.get("refs", []))

                elif field_schema["type"].startswith("list:basic:file:"):
                    for obj in value:
                        paths_to_check.add(Path(obj["file"]))
                        # paths_to_check.update(Path(ref) for ref in obj.get("refs", []))

                # if field_schema["type"].startswith("basic:dir:"):
                #     paths_to_check.add(Path(value["dir"]))
                #     paths_to_check.update(Path(ref) for ref in value.get("refs", []))

                # elif field_schema["type"].startswith("list:basic:dir:"):
                #     for obj in value:
                #         paths_to_check.add(Path(obj["dir"]))
                #         paths_to_check.update(Path(ref) for ref in obj.get("refs", []))

        return paths_to_check

    def check_output(self, data: Data) -> bool:
        """Check if all file outputs defined in output exist."""
        print(f"Checking data {data}, {data.pk}.")
        assert data.status == Data.STATUS_DONE, (
            f"Data with id {data.id} has invalid status"
            "{data.status}, expected {Data.STATUS_DONE}."
        )
        base_path = Path(data.location.subpath)
        paths = self.get_files_dirs(data.output, data.process.output_schema)
        for storage_location in data.location.storage_locations.exclude(
            connector_name="backup"
        ):
            for path in paths:
                path = base_path / path
                print(f"Checking {path}, location {storage_location}.")
                if not storage_location.connector.exists(path):
                    print(
                        f"File {path} is missing on Data objects with id "
                        f"{data.pk}, storage location {storage_location}."
                    )
                    return False
        return True

    def handle(self, *args, **kwargs):
        """Run test."""
        for data in Data.objects.filter(status=Data.STATUS_DONE).exclude(
            location__isnull=True
        ):
            if not self.check_output(data):
                return
        print("Check successfull.")
