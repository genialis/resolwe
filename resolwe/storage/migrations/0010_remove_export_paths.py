from django.db import migrations


def delete_nonexisting_paths(apps, schema_editor):
    """Remove referenced paths that point to exported files.

    Due to a bug in communication container references to all files are stored
    in the datase, not only to files in 'data' storage.

    Specifically the database contains references to files in the 'upload'
    storage, which are named 'export_#hex', where #hex is a 32 characters long
    hexadecimal stored as string.

    This scripts detects such paths and removes them if they do not exist in
    the referenced storage location (to make sure no legit similar named file
    is deleted).
    """
    ReferencedPath = apps.get_model("storage", "ReferencedPath")

    # Remove non-existing exported files paths from storage locations.
    for path in ReferencedPath.objects.filter(path__regex="export_[0-9a-f]{32}$"):
        for storage_location in path.storage_locations.all():
            connector = storage_location.connector
            url = f"{storage_location.file_storage_id}/{path.path}"
            if not connector.exists(url):
                storage_location.files.remove(path)

    # Delete all non-referenced paths.
    ReferencedPath.objects.filter(storage_locations__isnull=True).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("storage", "0008_accesslog_cause"),
    ]

    operations = [
        migrations.RunPython(delete_nonexisting_paths),
    ]
