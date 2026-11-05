import os

from django.db import connection, migrations, transaction

# The number of entities whose search vectors are recomputed in one transaction.
BATCH_SIZE = 1000

ANNOTATION_VALUE_TRIGGERS = [
    "annotationvalue_entity_ait",
    "annotationvalue_entity_aut",
    "annotationvalue_entity_adt",
]


def load_triggers(apps, schema_editor):
    file_names = ["triggers_entity.sql", "triggers_annotationvalue.sql"]
    with connection.cursor() as c:
        for file_name in file_names:
            file_path = os.path.join(os.path.dirname(__file__), file_name)
            with open(file_path) as fh:
                sql_statement = fh.read()
            c.execute(sql_statement)


def drop_annotation_value_triggers(apps, schema_editor):
    """Drop the triggers on annotation values.

    The entity search function keeps reading annotation values, because its
    previous definition is not stored anywhere.
    """
    with connection.cursor() as c:
        for name in ANNOTATION_VALUE_TRIGGERS:
            c.execute(f"DROP TRIGGER IF EXISTS {name} ON flow_annotationvalue;")
            c.execute(f"DROP FUNCTION IF EXISTS {name}();")


def recompute_entity_search(apps, schema_editor):
    """Recompute the search vectors of all entities in batches.

    Each batch runs in its own transaction to avoid locking all entities at once.
    """
    with connection.cursor() as c:
        c.execute("SELECT min(id), max(id) FROM flow_entity;")
        min_id, max_id = c.fetchone()
    if min_id is None:
        return
    for start in range(min_id, max_id + 1, BATCH_SIZE):
        with transaction.atomic(), connection.cursor() as c:
            # Setting the search field to NULL triggers entity_biut.
            c.execute(
                "UPDATE flow_entity SET search=NULL WHERE id >= %s AND id < %s;",
                [start, start + BATCH_SIZE],
            )


class Migration(migrations.Migration):
    atomic = False

    dependencies = [("flow", "0037_annotation_value_data_migrate")]

    operations = [
        migrations.RunPython(
            load_triggers, reverse_code=drop_annotation_value_triggers, atomic=True
        ),
        migrations.RunPython(
            recompute_entity_search, reverse_code=migrations.RunPython.noop
        ),
    ]
