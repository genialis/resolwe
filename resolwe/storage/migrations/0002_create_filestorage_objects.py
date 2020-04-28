import os

from django.db import migrations, connection
from django.conf import settings


def collect(base_dir):
    """Collect all files in base_dir."""
    collected_dirs = []
    collected_files = []
    for root, _, files in os.walk(os.path.join(base_dir)):
        # Make sure directory path ends with '/', do not include './'
        if root != base_dir:
            collected_dirs.append(os.path.join(os.path.relpath(root, base_dir), ""))
        for file_ in files:
            real_path = os.path.join(root, file_)
            file_size = os.path.getsize(real_path)
            collected_files.append((os.path.relpath(real_path, base_dir), file_size))
    return collected_files, collected_dirs


def create_new_storage_locations(apps, schema_editor):
    """Create FileStorage objects and StorageLocation from each
    DataLocation object. Make sure that ids of DataLocation and
    corresponding FileStorage objects are the same.

    Also collect files and assignes them to corresponding FileStorage object.
    Assumption: all files at the locotion are collected.
    """
    DataLocation = apps.get_model("flow", "DataLocation")
    FileStorage = apps.get_model("storage", "FileStorage")
    StorageLocation = apps.get_model("storage", "StorageLocation")
    ReferencedPath = apps.get_model("storage", "ReferencedPath")

    max_id = 1
    for data_location in DataLocation.objects.all():
        file_storage = FileStorage.objects.create(id=data_location.id)
        base_dir = os.path.join(
            settings.FLOW_EXECUTOR["DATA_DIR"], data_location.subpath
        )
        files, dirs = collect(base_dir)
        ReferencedPath.objects.bulk_create(
            [ReferencedPath(path=path, file_storage=file_storage) for path in dirs]
        )
        ReferencedPath.objects.bulk_create(
            [
                ReferencedPath(path=path, file_storage=file_storage, size=size)
                for path, size in files
            ]
        )
        StorageLocation.all_objects.create(
            id=data_location.id,
            file_storage=file_storage,
            url=data_location.subpath,
            status="OK",
            connector_name=getattr(settings, "STORAGE_LOCAL_CONNECTOR", "local"),
        )
        max_id = max(max_id, data_location.id + 1)

    # Increment FileStorage and StorageLocation id's sequence
    if FileStorage.objects.exists():
        with connection.cursor() as cursor:
            cursor.execute(
                "ALTER SEQUENCE storage_filestorage_id_seq RESTART WITH {};".format(
                    max_id
                )
            )
            cursor.execute(
                "ALTER SEQUENCE storage_storagelocation_id_seq RESTART WITH {};".format(
                    max_id
                )
            )


class Migration(migrations.Migration):
    dependencies = [
        ("storage", "0001_initial"),
        ("flow", "0043_full_text_search"),
    ]

    run_before = [("flow", "0044_datalocation_to_filestorage")]

    operations = [
        migrations.RunPython(create_new_storage_locations),
    ]
