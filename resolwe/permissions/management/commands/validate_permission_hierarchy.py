""".. Ignore pydocstyle D400.

=============================
Validate permission hierarchy
=============================

When data/entity object O is contained in container (entity/collection) C it
must inherit its permissions. This is implemented by connecting all objects in
a collection to the same PermissionGroup. However, there is no guarantee that
objects in the collection belong to the same permission group.

This command checks that for every container all objects it point to the same
PermissionGroup object.
"""

from django.core.management.base import BaseCommand

from resolwe.flow.models import Collection, Entity


class Command(BaseCommand):
    """Validate permissions hierarchy."""

    help = "Validate permissions hierarchy."

    def validate_collection(self, collection: Collection) -> bool:
        """Validate collection.

        Check that all objects inside the collection point to the same
        PermissionGroup object.
        """
        return any(
            queryset.exclude(permission_group=collection.permission_group)
            for queryset in (collection.data, collection.entity_set)
        )

    def validate_entity(self, entity: Entity) -> bool:
        """Validate entity.

        Check that all objects inside the collection point to the same
        PermissionGroup object.
        """
        return bool(entity.date.exclude(permission_group=entity.permission_group))

    def add_arguments(self, parser):
        """Command arguments."""
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-c",
            "--collection-slug",
            dest="collection_slug",
            help="Check only collection with the given slug.",
        )
        group.add_argument(
            "-e",
            "--entity-slug",
            dest="entity_slug",
            help="Check only entity with the given slug.",
        )

    def handle(self, **options):
        """Command handle."""
        collection_queryset = Collection.objects.none()
        entity_queryset = Entity.objects.none()
        if options["collection_slug"]:
            collection_queryset = collection_queryset.filter(
                slug=options["collection_slug"]
            )
        elif options["entity_slug"]:
            entity_queryset = entity_queryset.filter(slug=options["entity_slug"])
        else:
            Collection.objects.all()
            entity_queryset = Entity.objects.filter(collection__isnull=False)

        invalid_collections = [
            collection
            for collection in collection_queryset
            if not self.validate_collection(collection)
        ]
        invalid_entities = [
            entity for entity in entity_queryset if not self.validate_entity(entity)
        ]
        for collection in invalid_collections:
            self.stderr.write(
                f"Permission hierarchy mismatch in collection {collection.slug}"
            )

        for entity in invalid_entities:
            self.stderr.write(f"Permission hierarchy mismatch in entity {entity.slug}")

        if not (invalid_collections or invalid_entities):
            self.stdout.write("Check completed successfully.")
