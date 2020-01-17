from django.core.exceptions import ValidationError
from django.db import migrations

from django.utils.timezone import now

from resolwe.flow.utils import rewire_inputs


def copy_permissions(original, duplicate, model, apps):
    """Copy permissions from original object to it's duplicate."""
    UserObjectPermission = apps.get_model("guardian", "UserObjectPermission")
    GroupObjectPermission = apps.get_model("guardian", "GroupObjectPermission")

    kwargs = {
        "permission__content_type__model": model,
        "object_pk": original.id,
    }
    for perm in UserObjectPermission.objects.filter(**kwargs):
        perm.id = None
        perm.object_pk = duplicate.id
        perm.content_object = duplicate
        perm.save(force_insert=True)

    for perm in GroupObjectPermission.objects.filter(**kwargs):
        perm.id = None
        perm.object_pk = duplicate.id
        perm.content_object = duplicate
        perm.save(force_insert=True)


def duplicate_entity(original, apps):
    """Duplicate entity."""
    Entity = apps.get_model("flow", "Entity")

    duplicate = Entity.objects.get(id=original.id)
    duplicate.pk = None
    duplicate.slug = None
    duplicate.collection = None
    duplicate.name = "Copy of {}".format(original.name)
    duplicate.duplicated = now()

    duplicate.save(force_insert=True)

    copy_permissions(original, duplicate, model="entity", apps=apps)

    # Override fields that are automatically set on create.
    duplicate.created = original.created
    duplicate.save()

    # Duplicate entity's data objects.
    bundle = [
        {"original": obj, "copy": duplicate_data(obj, apps)}
        for obj in original.data.all()
    ]
    bundle = rewire_inputs(bundle)
    duplicated_data = [item["copy"] for item in bundle]

    duplicate.data.add(*duplicated_data)

    return duplicate


def duplicate_data(original, apps):
    """Duplicate (make a copy)."""
    DataDependency = apps.get_model("flow", "DataDependency")
    Data = apps.get_model("flow", "Data")

    if original.status not in ["OK", "ER"]:
        raise ValidationError(
            "Data object must have done or error status to be duplicated"
        )

    duplicate = Data.objects.get(id=original.id)
    duplicate.pk = None
    duplicate.slug = None
    duplicate.entity2 = None
    duplicate.collection = None
    duplicate.name = "Copy of {}".format(original.name)
    duplicate.duplicated = now()

    duplicate.save(force_insert=True)

    copy_permissions(original, duplicate, model="data", apps=apps)

    # Override fields that are automatically set on create.
    duplicate.created = original.created
    duplicate.save()

    if original.location:
        original.location.data.add(duplicate)

    duplicate.storages.set(original.storages.all())

    for migration in original.migration_history.order_by("created"):
        migration.pk = None
        migration.data = duplicate
        migration.save(force_insert=True)

    # Inherit existing child dependencies.
    DataDependency.objects.bulk_create(
        [
            DataDependency(
                child=duplicate, parent=dependency.parent, kind=dependency.kind
            )
            for dependency in DataDependency.objects.filter(child=original)
        ]
    )
    # Inherit existing parent dependencies.
    DataDependency.objects.bulk_create(
        [
            DataDependency(
                child=dependency.child, parent=duplicate, kind=dependency.kind
            )
            for dependency in DataDependency.objects.filter(parent=original)
        ]
    )

    return duplicate


def main(apps, schema_editor):
    """Reassign Data, Entities and Collections relations from m2m to m2o fields."""
    Data = apps.get_model("flow", "Data")
    Entity = apps.get_model("flow", "Entity")

    # 1: Migrate entities from multiple collections.
    for entity in Entity.objects.iterator():

        count = entity.collections.count()
        if count == 1:
            entity.collection = entity.collections.first()
            entity.save()
        elif count > 1:
            collections = entity.collections.all()
            for i, collection in enumerate(collections, start=1):
                # For the last collection, we can keep existing one.
                if i == count:
                    duplicate = entity
                else:
                    duplicate = duplicate_entity(entity, apps)

                duplicate.collection = collection
                duplicate.save()
                for datum in duplicate.data.all():
                    datum.collection = collection
                    datum.save()

    # 2: Migrate data from multiple collections.
    for data in Data.objects.iterator():

        count = data.collection_set.count()
        if count == 1:
            data.collection = data.collection_set.first()
            data.save()
        elif count > 1:
            collections = data.collection_set.all()
            for i, collection in enumerate(collections, start=1):
                # For the last collection, we can keep existing one.
                if i == count:
                    duplicate = data
                else:
                    duplicate = duplicate_data(data, apps)

                duplicate.collection = collection
                duplicate.save()

    # 3: Migrate data from multiple entities.
    for data in Data.objects.iterator():

        count = data.entity_set.count()
        if count == 1:
            data.entity2 = data.entity_set.first()
            data.save()
        elif count > 1:
            entities = data.entity_set.all()
            for i, entity in enumerate(entities, start=1):
                # For the last entity, we can keep existing one.
                if i == count:
                    duplicate = data
                else:
                    duplicate = duplicate_data(data, apps)

                duplicate.entity2 = entity
                duplicate.save()


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0036_add_m2o_fields"),
        ("contenttypes", "0002_remove_content_type_name"),
        ("guardian", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(main, atomic=True),
    ]
