import os

from django.db import connection, migrations, transaction

# The number of entities whose search vectors are recomputed in one transaction.
BATCH_SIZE = 1000

# Fail instead of queueing writers to the tables whose triggers are replaced.
LOCK_TIMEOUT = "5s"

ANNOTATION_VALUE_TRIGGERS = [
    "annotationvalue_entity_ait",
    "annotationvalue_entity_aut",
    "annotationvalue_entity_adt",
]


def execute_sql_file(cursor, file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path) as fh:
        cursor.execute(fh.read())


def recompute_entity_search():
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


def index_annotation_values(apps, schema_editor):
    with transaction.atomic(), connection.cursor() as c:
        c.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}';")
        execute_sql_file(c, "triggers_annotationvalue.sql")
    recompute_entity_search()


def unindex_annotation_values(apps, schema_editor):
    with transaction.atomic(), connection.cursor() as c:
        c.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}';")
        for name in ANNOTATION_VALUE_TRIGGERS:
            c.execute(f"DROP TRIGGER IF EXISTS {name} ON flow_annotationvalue;")
            c.execute(f"DROP FUNCTION IF EXISTS {name}();")
        c.execute("DROP FUNCTION IF EXISTS annotationvalue_entity_recompute(bigint[]);")
        execute_sql_file(c, "triggers_entity.sql")
    recompute_entity_search()


class Migration(migrations.Migration):
    atomic = False

    dependencies = [("flow", "0037_annotation_value_data_migrate")]

    operations = [
        migrations.RunPython(
            index_annotation_values, reverse_code=unindex_annotation_values
        ),
    ]
