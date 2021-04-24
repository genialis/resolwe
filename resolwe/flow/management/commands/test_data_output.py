""".. Ignore pydocstyle D400.

==============================================================
Test if required file outputs of completed Data objects exists
==============================================================

Command to run on local machine::

    ./manage.py test_data_output

"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Set

from django.core.management.base import BaseCommand

from resolwe.flow.models import Data
from resolwe.flow.utils import iterate_fields

logger = logging.getLogger(__name__)

EXCLUDED_STORAGE_LOCATIONS = ["backup"]
LOG_PROGRESS = 5  # Log progress every LOG_PROGRESS%.


class Command(BaseCommand):
    """Test if file outputs of completed Data objects are referenced.

    We only check that the outputs are referenced in the database. The actual existence
    of the files has to be checked separately (see verify_data method on StoraLocation).
    """

    help = "Test if file outputs of completed Data objects exists."

    def get_files_dirs(
        self, output: dict, output_schema: dict
    ) -> tuple[set[str], set[str]]:
        """Get a set of files and dirs for a given output."""

        def normalize(path: str) -> str:
            """Normalize the path.

            For instance './file.txt' is translated into 'file.txt'.
            """
            return str(Path(path))

        paths_to_check: Set[str] = set()
        references: Set[str] = set()
        for field_schema, fields in iterate_fields(output, output_schema):
            name = field_schema["name"]
            value = fields[name]
            if "type" in field_schema:
                if field_schema["type"].startswith("basic:file:"):
                    paths_to_check.add(normalize(value["file"]))
                    references.update(normalize(ref) for ref in value.get("refs", []))

                elif field_schema["type"].startswith("list:basic:file:"):
                    for obj in value:
                        paths_to_check.add(normalize(obj["file"]))
                        references.update(normalize(ref) for ref in obj.get("refs", []))

                if field_schema["type"].startswith("basic:dir:"):
                    # Make sure '/' is appended if the path represents a directory.
                    to_add = normalize(value["dir"])
                    if not to_add.endswith("/"):
                        to_add += "/"
                    paths_to_check.add(to_add)
                    references.update(normalize(ref) for ref in value.get("refs", []))

                elif field_schema["type"].startswith("list:basic:dir:"):
                    for obj in value:
                        # Make sure '/' is appended if the path represents a directory.
                        to_add = normalize(obj["dir"])
                        if not to_add.endswith("/"):
                            to_add += "/"
                        paths_to_check.add(to_add)
                        references.update(
                            normalize(ref) for ref in value.get("refs", [])
                        )
        return paths_to_check, references

    def check_output(self, data: Data) -> bool:
        """Check if all files in data output are referenced."""
        logger.debug("Checking data with id %s.", data.id)
        assert data.status == Data.STATUS_DONE, (
            f"Data with id {data.id} has invalid status"
            "{data.status}, expected {Data.STATUS_DONE}."
        )
        assert data.location is not None, f"Data with id {data.id} has no location."
        paths, references = self.get_files_dirs(data.output, data.process.output_schema)

        # Check that the output files are referenced on all storage locations except backup.
        for storage_location in data.location.storage_locations.exclude(
            connector_name__in=EXCLUDED_STORAGE_LOCATIONS
        ):
            for path in paths:
                logger.debug(f"Checking {path}, location {storage_location}.")
                if not storage_location.files.filter(path=path).exists():
                    logger.error(
                        f"File {path} is missing on Data objects with id "
                        f"{data.pk}, storage location {storage_location}."
                    )
                    return False
            for reference in references:
                logger.debug(
                    f"Checking reference {reference}, location {storage_location}."
                )
                # Check that the reference exists, check for both file and directory.
                if (
                    not storage_location.files.filter(path=reference).exists()
                    and not storage_location.files.filter(path=reference + "/").exists()
                ):
                    logger.error(
                        f"Reference {reference} is missing on Data objects with id "
                        f"{data.pk}, storage location {storage_location}."
                    )
                    return False
        return True

    def add_arguments(self, parser):
        """Command arguments."""
        parser.add_argument(
            "-d",
            "--data",
            dest="data_ids",
            nargs="+",
            type=int,
            help="IDs of the data objects to check. Defaults to all completed data objects.",
            default=[],
        )

    def handle(self, *args, **kwargs):
        """Run test."""
        filters = {}
        if data_ids := kwargs["data_ids"]:
            filters = {"pk__in": data_ids}

        processed_location_ids: dict[int, list[int]] = defaultdict(list)
        failed_location_ids: set[int] = set()

        queryset = Data.objects.filter(status=Data.STATUS_DONE, **filters).exclude(
            location__isnull=True
        )

        to_process = queryset.count()
        previous_progress = 0.0
        progress = 0.0

        logger.info("Checking %d data objects.", to_process)
        for number, data in enumerate(queryset, start=1):
            logger.debug("Processing data %s/%s.", number, to_process)

            if data.location_id in processed_location_ids:
                logger.debug(
                    "Location of data with id %s already processed by data objects %s. Skipping.",
                    data.location_id,
                    processed_location_ids[data.location_id],
                )
            elif not self.check_output(data):
                failed_location_ids.add(data.location_id)
            processed_location_ids[data.location_id].append(data.id)

            # Log progress every LOG_PROGRESS%.
            progress = (number / to_process) * 100
            if progress - previous_progress >= LOG_PROGRESS:
                logger.info("Progress: %.2f%%", progress)
                previous_progress = progress

        if failed_location_ids:
            logger.error(
                "There are data objects with location IDs %s that have missing files. "
                "See error messages above for more information.",
                failed_location_ids,
            )
        else:
            logger.info("All files on outputs are present.")
