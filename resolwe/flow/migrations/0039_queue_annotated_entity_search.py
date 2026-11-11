from django.db import connection, migrations, transaction

# The number of entities whose search vectors are rebuilt in one transaction when
# the migration is reversed.
BATCH_SIZE = 1000


def queue_annotated_entities(apps, schema_editor):
    """Queue the entities with annotation values for a search rebuild.

    The listener rebuilds their search vectors. The vectors of entities without
    annotation values do not change.
    """
    with connection.cursor() as c:
        c.execute(
            "INSERT INTO flow_entitysearchqueue (entity_id, created) "
            "SELECT DISTINCT entity_id, now() FROM flow_annotationvalue "
            "WHERE deleted = false;"
        )


def remove_annotations_from_search(apps, schema_editor):
    """Rebuild the search vectors of annotated entities without annotations.

    Without generate_resolwe_entity_annotation_search the entity search function
    no longer appends annotation values.
    """
    with transaction.atomic(), connection.cursor() as c:
        c.execute("SET LOCAL lock_timeout = '5s';")
        c.execute(
            "DROP FUNCTION IF EXISTS "
            "generate_resolwe_entity_annotation_search(flow_entity);"
        )
    with connection.cursor() as c:
        c.execute(
            "SELECT DISTINCT entity_id FROM flow_annotationvalue "
            "WHERE deleted = false ORDER BY entity_id;"
        )
        entity_ids = [row[0] for row in c.fetchall()]
    for start in range(0, len(entity_ids), BATCH_SIZE):
        batch = entity_ids[start : start + BATCH_SIZE]
        with transaction.atomic(), connection.cursor() as c:
            c.execute("SET LOCAL lock_timeout = '5s';")
            c.execute(
                "SELECT id FROM flow_entity WHERE id = ANY(%s) "
                "ORDER BY id FOR NO KEY UPDATE;",
                [batch],
            )
            # Setting the search field to NULL triggers entity_biut.
            c.execute(
                "UPDATE flow_entity SET search = NULL WHERE id = ANY(%s);", [batch]
            )


class Migration(migrations.Migration):
    """Queue the entities with annotation values for a search rebuild.

    Separate from 0038, so the scan of the annotation values does not hold the
    locks taken while the triggers are replaced. The reverse rebuilds in batches
    that commit, so it is not atomic.
    """

    atomic = False

    dependencies = [("flow", "0038_annotation_values_in_entity_search")]

    operations = [
        migrations.RunPython(
            queue_annotated_entities, reverse_code=remove_annotations_from_search
        ),
    ]
