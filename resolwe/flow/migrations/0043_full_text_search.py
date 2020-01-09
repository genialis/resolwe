# Generated by Django 2.2.9 on 2020-01-10 16:44
import os

import django.contrib.postgres.indexes
import django.contrib.postgres.search
from django.contrib.postgres.operations import TrigramExtension
from django.db import connection, migrations, models


def load_triggers(apps, schema_editor):
    file_names = [
        "utils.sql",
        "triggers_collection.sql",
        "triggers_entity.sql",
        "triggers_data.sql",
    ]
    with connection.cursor() as c:
        for file_name in file_names:
            file_path = os.path.join(os.path.dirname(__file__), file_name)
            with open(file_path) as fh:
                sql_statement = fh.read()
            c.execute(sql_statement)


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0042_delete_obsolete_perms"),
    ]

    operations = [
        TrigramExtension(),
        migrations.AddField(
            model_name="collection",
            name="search",
            field=django.contrib.postgres.search.SearchVectorField(null=True),
        ),
        migrations.AddField(
            model_name="data",
            name="search",
            field=django.contrib.postgres.search.SearchVectorField(null=True),
        ),
        migrations.AddField(
            model_name="entity",
            name="search",
            field=django.contrib.postgres.search.SearchVectorField(null=True),
        ),
        migrations.RunPython(load_triggers),
        # Update existing entries.
        migrations.RunSQL("UPDATE flow_collection SET id=id;"),
        migrations.RunSQL("UPDATE flow_entity SET id=id;"),
        migrations.RunSQL("UPDATE flow_data SET id=id;"),
        migrations.AddIndex(
            model_name="collection",
            index=models.Index(fields=["name"], name="idx_collection_name"),
        ),
        migrations.AddIndex(
            model_name="collection",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["name"],
                name="idx_collection_name_trgm",
                opclasses=["gin_trgm_ops"],
            ),
        ),
        migrations.AddIndex(
            model_name="collection",
            index=models.Index(fields=["slug"], name="idx_collection_slug"),
        ),
        migrations.AddIndex(
            model_name="collection",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["tags"], name="idx_collection_tags"
            ),
        ),
        migrations.AddIndex(
            model_name="collection",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["search"], name="idx_collection_search"
            ),
        ),
        migrations.AddIndex(
            model_name="data",
            index=models.Index(fields=["name"], name="idx_data_name"),
        ),
        migrations.AddIndex(
            model_name="data",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["name"], name="idx_data_name_trgm", opclasses=["gin_trgm_ops"]
            ),
        ),
        migrations.AddIndex(
            model_name="data",
            index=models.Index(fields=["slug"], name="idx_data_slug"),
        ),
        migrations.AddIndex(
            model_name="data",
            index=models.Index(fields=["status"], name="idx_data_status"),
        ),
        migrations.AddIndex(
            model_name="data",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["tags"], name="idx_data_tags"
            ),
        ),
        migrations.AddIndex(
            model_name="data",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["search"], name="idx_data_search"
            ),
        ),
        migrations.AddIndex(
            model_name="entity",
            index=models.Index(fields=["name"], name="idx_entity_name"),
        ),
        migrations.AddIndex(
            model_name="entity",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["name"], name="idx_entity_name_trgm", opclasses=["gin_trgm_ops"]
            ),
        ),
        migrations.AddIndex(
            model_name="entity",
            index=models.Index(fields=["slug"], name="idx_entity_slug"),
        ),
        migrations.AddIndex(
            model_name="entity",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["tags"], name="idx_entity_tags"
            ),
        ),
        migrations.AddIndex(
            model_name="entity",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["search"], name="idx_entity_search"
            ),
        ),
        migrations.AddIndex(
            model_name="process",
            index=models.Index(fields=["name"], name="idx_process_name"),
        ),
        migrations.AddIndex(
            model_name="process",
            index=models.Index(fields=["slug"], name="idx_process_slug"),
        ),
        migrations.AddIndex(
            model_name="process",
            index=models.Index(fields=["type"], name="idx_process_type"),
        ),
        migrations.AddIndex(
            model_name="process",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["type"],
                name="idx_process_type_trgm",
                opclasses=["gin_trgm_ops"],
            ),
        ),
    ]
