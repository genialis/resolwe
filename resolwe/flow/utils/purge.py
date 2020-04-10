""".. Ignore pydocstyle D400.

==========
Data Purge
==========

"""
import logging
import os
import shutil

from django.conf import settings
from django.db.models import Q

from resolwe.flow.models import Data, Storage
from resolwe.flow.utils import iterate_fields
from resolwe.storage.models import FileStorage
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)


def get_purge_files(root, output, output_schema, descriptor, descriptor_schema):
    """Get files to purge."""

    def remove_file(fn, paths):
        """From paths remove fn and dirs before fn in dir tree."""
        while fn:
            for i in range(len(paths) - 1, -1, -1):
                if fn == paths[i]:
                    paths.pop(i)
            fn, _ = os.path.split(fn)

    def remove_tree(fn, paths):
        """From paths remove fn and dirs before or after fn in dir tree."""
        for i in range(len(paths) - 1, -1, -1):
            head = paths[i]
            while head:
                if fn == head:
                    paths.pop(i)
                    break
                head, _ = os.path.split(head)

        remove_file(fn, paths)

    def subfiles(root):
        """Extend unreferenced list with all subdirs and files in top dir."""
        subs = []
        for path, dirs, files in os.walk(root, topdown=False):
            path = path[len(root) + 1 :]
            subs.extend(os.path.join(path, f) for f in files)
            subs.extend(os.path.join(path, d) for d in dirs)
        return subs

    unreferenced_files = subfiles(root)

    remove_file("jsonout.txt", unreferenced_files)
    remove_file("stderr.txt", unreferenced_files)
    remove_file("stdout.txt", unreferenced_files)

    meta_fields = [[output, output_schema], [descriptor, descriptor_schema]]

    for meta_field, meta_field_schema in meta_fields:
        for field_schema, fields in iterate_fields(meta_field, meta_field_schema):
            if "type" in field_schema:
                field_type = field_schema["type"]
                field_name = field_schema["name"]

                # Remove basic:file: entries
                if field_type.startswith("basic:file:"):
                    remove_file(fields[field_name]["file"], unreferenced_files)

                # Remove list:basic:file: entries
                elif field_type.startswith("list:basic:file:"):
                    for field in fields[field_name]:
                        remove_file(field["file"], unreferenced_files)

                # Remove basic:dir: entries
                elif field_type.startswith("basic:dir:"):
                    remove_tree(fields[field_name]["dir"], unreferenced_files)

                # Remove list:basic:dir: entries
                elif field_type.startswith("list:basic:dir:"):
                    for field in fields[field_name]:
                        remove_tree(field["dir"], unreferenced_files)

                # Remove refs entries
                if field_type.startswith("basic:file:") or field_type.startswith(
                    "basic:dir:"
                ):
                    for ref in fields[field_name].get("refs", []):
                        remove_tree(ref, unreferenced_files)

                elif field_type.startswith("list:basic:file:") or field_type.startswith(
                    "list:basic:dir:"
                ):
                    for field in fields[field_name]:
                        for ref in field.get("refs", []):
                            remove_tree(ref, unreferenced_files)

    return set([os.path.join(root, filename) for filename in unreferenced_files])


def location_purge(location_id, delete=False, verbosity=0):
    """Print and conditionally delete files not referenced by meta data.

    :param location_id: Id of the
        :class:`~resolwe.storage.models.FileStorage` model that data
        objects reference to.
    :param delete: If ``True``, then delete unreferenced files.
    """
    try:
        location = FileStorage.objects.get(id=location_id)
    except FileStorage.DoesNotExist:
        logger.warning(
            "FileStorage location does not exist", extra={"location_id": location_id}
        )
        return

    unreferenced_files = set()
    purged_data = Data.objects.none()
    referenced_by_data = location.data.exists()
    if referenced_by_data:
        if location.data.exclude(
            status__in=[Data.STATUS_DONE, Data.STATUS_ERROR]
        ).exists():
            return

        # Perform cleanup.
        purge_files_sets = list()
        purged_data = location.data.all()
        for data in purged_data:
            purge_files_sets.append(
                get_purge_files(
                    location.get_path(),
                    data.output,
                    data.process.output_schema,
                    data.descriptor,
                    getattr(data.descriptor_schema, "schema", []),
                )
            )

        intersected_files = (
            set.intersection(*purge_files_sets) if purge_files_sets else set()
        )
        unreferenced_files.update(intersected_files)
    else:
        # Remove data directory.
        unreferenced_files.add(location.get_path())
        unreferenced_files.add(
            os.path.join(settings.FLOW_EXECUTOR["RUNTIME_DIR"], location.subpath)
        )

    if verbosity >= 1:
        # Print unreferenced files
        if unreferenced_files:
            logger.info(
                __(
                    "Unreferenced files for location id {} ({}):",
                    location_id,
                    len(unreferenced_files),
                )
            )
            for name in unreferenced_files:
                logger.info(__("  {}", name))
        else:
            logger.info(__("No unreferenced files for location id {}", location_id))

    # Go through unreferenced files and delete them.
    if delete:
        for name in unreferenced_files:
            if os.path.isfile(name) or os.path.islink(name):
                os.remove(name)
            elif os.path.isdir(name):
                shutil.rmtree(name)

        location.purged = True
        location.save()

        if not referenced_by_data:
            # TODO: what should I do here? Delete ell storage locations?
            # I should know more about purge to actually make this decision.
            location.storage_locations.all().delete()
            location.delete()


def _location_purge_all(delete=False, verbosity=0):
    """Purge all data locations."""
    if FileStorage.objects.exists():  # TODO: only default storage location is purged
        for location in FileStorage.objects.filter(Q(purged=False) | Q(data=None)):
            location_purge(location.id, delete, verbosity)
    else:
        logger.info("No data locations")


def _storage_purge_all(delete=False, verbosity=0):
    """Purge unreferenced storages."""
    orphaned_storages = Storage.objects.filter(data=None)

    if verbosity >= 1:
        if orphaned_storages.exists():
            logger.info(__("Unreferenced storages ({}):", orphaned_storages.count()))
            for storage_id in orphaned_storages.values_list("id", flat=True):
                logger.info(__("  {}", storage_id))
        else:
            logger.info("No unreferenced storages")

    if delete:
        orphaned_storages.delete_chunked()


def purge_all(delete=False, verbosity=0):
    """Purge all data locations."""
    _location_purge_all(delete, verbosity)
    _storage_purge_all(delete, verbosity)
