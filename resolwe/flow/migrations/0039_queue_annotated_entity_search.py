import os

from django.db import migrations, transaction

# The number of entities whose search vectors are rebuilt in one transaction when
# the migration is reversed.
BATCH_SIZE = 1000


def execute_sql_file(cursor, file_name):
    file_path = os.path.join(os.path.dirname(__file__), file_name)
    with open(file_path) as fh:
        cursor.execute(fh.read())


def queue_annotated_entities(apps, schema_editor):
    """Index annotation values and queue the annotated entities for a rebuild.

    Creating the function takes no table lock. It commits before the annotation
    values are scanned, so a failed or cancelled scan cannot roll it back: the
    function matters more than the queue, since without it every rebuild leaves
    the annotation values out. Running this again is safe. The listener rebuilds
    the search vectors of the queued entities. The vectors of entities without
    annotation values do not change.
    """
    connection = schema_editor.connection
    with transaction.atomic(using=connection.alias), connection.cursor() as c:
        execute_sql_file(c, "function_entity_annotation_search.sql")
    with transaction.atomic(using=connection.alias), connection.cursor() as c:
        c.execute(
            "INSERT INTO flow_entitysearchqueue (entity_id, created) "
            "SELECT DISTINCT entity_id, now() FROM flow_annotationvalue "
            "WHERE deleted = false;"
        )


def remove_annotations_from_search(apps, schema_editor):
    """Rebuild the search vectors of annotated entities without annotations.

    Without generate_resolwe_entity_annotation_search the entity search function
    no longer appends annotation values. The entities still queued are rebuilt
    too, since their vectors may hold annotation values deleted since the last
    rebuild. A reverse that fails or is interrupted, also with Ctrl-C, restores the
    function and queues the annotated entities again, so the database matches the
    applied migration. A lost connection, a killed process, or an interrupt after
    the last batch but before Django records the migration as unapplied can still
    leave the function dropped while 0039 is applied; run the reverse again then.
    """
    connection = schema_editor.connection
    # The handler also covers the DROP, since an interrupt can arrive after its
    # commit has reached the server. Restoring is safe to repeat. A
    # KeyboardInterrupt is not an Exception.
    try:
        with transaction.atomic(using=connection.alias), connection.cursor() as c:
            c.execute("SET LOCAL lock_timeout = '5s';")
            c.execute(
                "DROP FUNCTION IF EXISTS "
                "generate_resolwe_entity_annotation_search(flow_entity);"
            )
        with connection.cursor() as c:
            c.execute(
                "SELECT entity_id FROM flow_annotationvalue WHERE deleted = false "
                "UNION SELECT entity_id FROM flow_entitysearchqueue "
                "ORDER BY entity_id;"
            )
            entity_ids = [row[0] for row in c.fetchall()]
        for start in range(0, len(entity_ids), BATCH_SIZE):
            batch = entity_ids[start : start + BATCH_SIZE]
            with transaction.atomic(using=connection.alias), connection.cursor() as c:
                c.execute("SET LOCAL lock_timeout = '5s';")
                c.execute(
                    "SELECT id FROM flow_entity WHERE id = ANY(%s) "
                    "ORDER BY id FOR NO KEY UPDATE;",
                    [batch],
                )
                # Setting the search field to NULL triggers entity_biut.
                c.execute(
                    "UPDATE flow_entity SET search = NULL WHERE id = ANY(%s);",
                    [batch],
                )
    except BaseException:
        queue_annotated_entities(apps, schema_editor)
        raise


class Migration(migrations.Migration):
    """Index annotation values and queue the annotated entities for a rebuild.

    Separate from 0038, so the scan of the annotation values does not hold the
    lock that 0038 takes while it creates the triggers. The function that indexes
    the annotation values is created here and dropped by the reverse, so a reverse
    followed by a forward migration indexes them again. The reverse rebuilds in
    batches that commit, so it is not atomic.
    """

    atomic = False

    dependencies = [("flow", "0038_annotation_values_in_entity_search")]

    operations = [
        migrations.RunPython(
            queue_annotated_entities, reverse_code=remove_annotations_from_search
        ),
    ]
