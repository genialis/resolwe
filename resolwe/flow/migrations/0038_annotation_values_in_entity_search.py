import os

from django.db import connection, migrations, models

ANNOTATION_VALUE_TRIGGERS = [
    "annotationvalue_entity_ait",
    "annotationvalue_entity_aut",
    "annotationvalue_entity_adt",
]


def execute_sql_file(cursor, file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path) as fh:
        cursor.execute(fh.read())


def load_triggers(apps, schema_editor):
    with connection.cursor() as c:
        execute_sql_file(c, "triggers_entity.sql")
        execute_sql_file(c, "triggers_annotationvalue.sql")


def drop_triggers(apps, schema_editor):
    with connection.cursor() as c:
        for name in ANNOTATION_VALUE_TRIGGERS:
            c.execute(f"DROP TRIGGER IF EXISTS {name} ON flow_annotationvalue;")
            c.execute(f"DROP FUNCTION IF EXISTS {name}();")
        c.execute(
            "DROP FUNCTION IF EXISTS "
            "generate_resolwe_entity_annotation_search(flow_entity);"
        )


class Migration(migrations.Migration):
    """Queue entities whose annotation values change for a search rebuild.

    The migration is atomic, so a lock timeout while the triggers are replaced
    rolls everything back and the migration can be run again.
    """

    dependencies = [("flow", "0037_annotation_value_data_migrate")]

    operations = [
        migrations.CreateModel(
            name="EntitySearchQueue",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("entity_id", models.IntegerField(db_index=True)),
                ("created", models.DateTimeField(auto_now_add=True)),
            ],
        ),
        # Fail instead of queueing writers to the tables whose triggers are
        # replaced. The second operation sets the timeout when reversing, since
        # the operations then run in reverse order.
        migrations.RunSQL(
            "SET LOCAL lock_timeout = '5s';", reverse_sql=migrations.RunSQL.noop
        ),
        migrations.RunPython(load_triggers, reverse_code=drop_triggers),
        migrations.RunSQL(
            migrations.RunSQL.noop, reverse_sql="SET LOCAL lock_timeout = '5s';"
        ),
    ]
