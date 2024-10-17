"""Resolwe models duplicate utils."""

from copy import deepcopy
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Sequence, Type, TypeVar

from django.core.exceptions import ValidationError
from django.db import IntegrityError, models, transaction
from django.utils import timezone

from resolwe.flow.models import AnnotationField, AnnotationValue
from resolwe.flow.utils import iterate_fields
from resolwe.permissions.models import Permission, PermissionGroup, PermissionObject
from resolwe.permissions.utils import assign_contributor_permissions, copy_permissions

if TYPE_CHECKING:
    from resolwe.flow.models import Collection, Data, Entity
    from resolwe.flow.models.collection import CollectionQuerySet
    from resolwe.flow.models.data import DataQuerySet
    from resolwe.flow.models.entity import EntityQuerySet

DataPK = int
DataPKMap = dict[DataPK, DataPK]
CollectionPK = int
CollectionPKMap = dict[CollectionPK, "Collection"]
EntityPK = int
EntityPKMap = dict[EntityPK, "Entity"]


T = TypeVar("T", "Data", "Collection", "Entity")
U = TypeVar("U", bound=models.Model)


def send_duplicate_signal(
    model: Type[models.Model],
    new_objects: Sequence[models.Model],
    objects: Sequence[models.Model],
):
    """Send post_duplicate signal."""
    from resolwe.flow.signals import post_duplicate

    post_duplicate.send(sender=model, instances=new_objects, old_instances=objects)


def _rewire_data_relations(
    old_data: Sequence["Data"], new_data: Sequence["Data"]
) -> None:
    """Rewire the relations on the new objects based on old ones."""

    # Prevent circular import.
    from resolwe.flow.models import Data, DataDependency, DataMigrationHistory

    pk_mapping: dict[int, int] = {
        old.pk: new.pk for old, new in zip(old_data, new_data)
    }
    _rewire_inputs(new_data, pk_mapping)

    # Add m2m storage relations.
    through_model = Data.storages.through  # type: ignore
    storage_relations = through_model.objects.filter(data__in=old_data)

    _rewire_foreign_key(storage_relations, Data, pk_mapping)

    # Copy data dependencies.
    # We only inherit parent dependencies to keep track of the Data object's
    # source. Child dependencies reflect later actions that don't directly
    # depend on the current object, so copying them would only make unnecessary
    # noise. Duplicate dependencies are not copied to keep a simple chain with
    # links only to direct predecessors.
    data_dependencies = DataDependency.objects.filter(
        kind__in=[DataDependency.KIND_IO, DataDependency.KIND_SUBPROCESS],
        child__in=old_data,
    )
    _rewire_foreign_key(data_dependencies, Data, pk_mapping)

    # Add duplicate dependencies.
    DataDependency.objects.bulk_create(
        DataDependency(parent=old, child=new, kind=DataDependency.KIND_DUPLICATE)
        for old, new in zip(old_data, new_data)
    )

    # Copy migration history. Ordering is needed to keep the order of migrations.
    migration_history = DataMigrationHistory.objects.filter(data__in=old_data).order_by(
        "pk"
    )
    _rewire_foreign_key(migration_history, Data, pk_mapping)


def _rewire_foreign_key(
    objects: models.QuerySet, target: Type[models.Model], pk_mapping: DataPKMap
) -> list:
    """Make a copy of given objects and rewire references to target objects.

    All fields that points to an object of ``target`` type are replaced with
    the new objects according to the provided mapping. The list of new objects
    is returned. The new objects are already saved to the database.

    :param objects: A list of objects to process.
    :type objects: list of `~django.db.models.Model`

    :param target: A class to which references should be replaced
    :type terget: `~django.db.models.Model`

    :param dict pk_mapping: Dictionary with old primary keys as keys and new
        primary keys as values.

    :rtype: list of `~django.db.models.Model`

    """
    if not objects:
        return []

    # Check that all objects are instances of the same class.
    obj_class = type(objects[0])
    if any(type(obj) != obj_class for obj in objects):
        raise ValueError("All objects must be instances of the same class.")

    # Find fields pointing to the target model.
    to_map = [
        field.name
        for field in obj_class._meta.fields
        if type(field) == models.ForeignKey and field.related_model == target
    ]

    # Rewire references.
    new_objects = []
    for obj in objects:
        new_obj = deepcopy(obj)
        for field_name in to_map:
            field_name_id = f"{field_name}_id"
            if value := pk_mapping.get(getattr(obj, field_name_id)):
                setattr(new_obj, field_name_id, value)

        new_obj.pk = None
        new_objects.append(new_obj)

    return obj_class.objects.bulk_create(new_objects)


def _rewire_inputs(data: Iterable["Data"], pk_mapping: dict[int, int]) -> None:
    """Rewire inputs of provided data objects.

    References to input data objects in ``input`` field of the Data objects are changed
    with references of their copies according to the provided mapping. If primary key
    is not in the mapping, original value is kept.

    The data objects are saved to the database using bulk_update method so no signal is
    triggered.

    :param list data: A list of Data objects to process.

    :param dict pk_mapping: A dict where keys are primary keys of original Data objects
        and values are primary keys of their copies.

    """
    from resolwe.flow.models import Data

    for datum in data:
        for field_schema, fields in iterate_fields(
            datum.input, datum.process.input_schema
        ):
            name = field_schema["name"]
            value = fields[name]

            if field_schema["type"].startswith("data:") and value in pk_mapping:
                fields[name] = pk_mapping[value]

            elif field_schema["type"].startswith("list:data:"):
                fields[name] = [
                    pk_mapping[pk] if pk in pk_mapping else pk for pk in value
                ]
    Data.objects.bulk_update(data, ["input"])


def _check_permissions(
    objects: Iterable[PermissionObject], permission: Permission, user
) -> None:
    """Check that user has permission on all given objects.

    :param objects: An iterable of objects to be checked.
    :param permission: the required permission.
    :raises: ValidationError when user does not have all the required
        permissions.
    """
    # The permissions are the same for objects in the same permission groups.
    # So only check permissions for objects belonging to the different
    # permission groups.
    # It does not matter which object for the permission group we check so we
    # pick out the last one.

    for obj in {obj.permission_group_id: obj for obj in objects}.values():
        if not user.has_perm(permission, obj):
            raise ValidationError(
                f"User doesn't have '{permission}' permission on {obj}."
            )


def _check_created_objects_permissions(
    new_data: Iterable["Data"],
    new_entities: Iterable["Entity"],
    inherit_collection: bool,
    inherit_entity: bool,
    contributor,
):
    """Check that new objects can be created in the collections."""

    from resolwe.flow.models import Collection, Entity

    if inherit_collection:
        data_collections = Collection.objects.filter(data__in=new_data).distinct()
        _check_permissions(data_collections, Permission.EDIT, contributor)
        if new_entities:
            entity_collections = (
                Collection.objects.filter(entity__in=new_entities)
                .exclude(data__in=new_data)
                .distinct()
            )
            _check_permissions(entity_collections, Permission.EDIT, contributor)

    if inherit_entity:
        data_entities = Entity.objects.filter(data__in=new_data).distinct()
        _check_permissions(data_entities, Permission.EDIT, contributor)


def process_entity(
    inherit_collection: bool, collection_mapping: CollectionPKMap
) -> Callable[["Entity"], "Entity"]:
    """Return a function that process a duplicated Entity before saving.

    :param inherit_collection: Indicates whether the collection should be inherited or
        not.

    :param collection_mapping: A dict where keys are primary keys of original
        collections and values are primary keys of their copies.
    """

    def processor(entity: "Entity") -> "Entity":
        """Set collection of given Entity.

        Collection is set to ``None`` if ``inherit_collection`` value is set to
        ``False``, or mapped according to the ``collection_mapping`` otherwise (or left
        as is if value is not in the mapping).

        :param entity: An entity to be processed.
        """
        if inherit_collection and entity.collection:
            if entity.collection.id in collection_mapping:
                entity.collection = collection_mapping[entity.collection.pk]
        else:
            entity.collection = None

        return entity

    return processor


def process_data(
    inherit_collection: bool,
    inherit_entity: bool,
    collection_mapping: CollectionPKMap = dict(),
    entity_mapping: EntityPKMap = dict(),
) -> Callable[["Data"], "Data"]:
    """Return a function that process a duplicated Data object before saving.

    :param inherit_collection: Indicates whether the collection should be inherited.

    :param inherit_entity: Indicates whether the entity should be inherited.

    :param collection_mapping: A dict where keys are primary keys of original
        collections and values are primary keys of their copies.

    :param entity_mapping: A dict where keys are primary keys of original entities and
        values are primary keys of their copies.
    """

    def processor(datum: "Data") -> "Data":
        """Set collection and entity of given Data object.

        Collection and entity are set to ``None`` if ``inherit_collection`` and
        ``inherit_entity`` values are set to ``False``, respectively, and mapped
        according to ``collection_mapping`` and ``entity_mapping`` otherwise (or left
        as is if value is not in the mapping).

        :param datum: An Data object to be processed.
        """
        if inherit_collection and datum.collection:
            if datum.collection.id in collection_mapping:
                datum.collection = collection_mapping[datum.collection.pk]
        else:
            datum.collection = None

        if inherit_entity and datum.entity:
            if datum.entity.id in entity_mapping:
                datum.entity = entity_mapping[datum.entity.pk]
        else:
            datum.entity = None

        return datum

    return processor


def copy_objects(
    objects: Sequence[T],
    contributor,
    name_prefix: str,
    obj_processor: Optional[Callable[[T], T]] = None,
    send_signals: bool = True,
) -> list[T]:
    """Make a copy of given queryset.

    Shallow copy given queryset and set contributor to the given value, prepend name
    with the prefix, set slug to a unique value, and set ``duplicated`` date to the
    current time. Special attention is paid to keep the ``created`` date to its
    original value.

    If ``obj_processor`` function is given, each object is passed to it and its return
    value is used instead of it.

    :param objects: A queryset to be copied.

    :param contributor: A Django user that will be assigned to copied objects as
        contributor.

    :param name_prefix: A prefix that will be prepend to the name of copied objects.

    :param send_signals: Should a post_duplicate signal be sent after copying objects.
    """

    if not objects:
        return []

    name_prefix = name_prefix or "Copy of"

    first = objects[0]
    name_max_length: int = first._meta.get_field("name").max_length  # type: ignore
    model = first._meta.model

    new_objects: list[T] = []
    for obj in objects:
        new_obj = deepcopy(obj)
        new_obj.pk = None
        new_obj.slug = None
        new_obj.contributor = contributor
        new_obj.name = f"{name_prefix} {obj.name}"
        # Truncate name if necessary.
        if len(new_obj.name) > name_max_length:
            # Truncate to name_max_length-3 and pad with dots until name_max_length.
            new_obj.name = f"{new_obj.name:.<{name_max_length}.{name_max_length-3}}"

        if obj_processor:
            new_obj = obj_processor(new_obj)

        new_objects.append(new_obj)

    try:
        # Add another atomic block to avoid corupting the main one.
        with transaction.atomic():
            model.objects.bulk_create(new_objects)
    except IntegrityError:
        # Probably a slug collision occured, try to create objects one by one.
        for obj in new_objects:
            obj.slug = None
            # Call the bulk_create to skip signal sending.
            model.objects.bulk_create([obj])

    # Send the signal after the transaction is commited.
    if send_signals:
        transaction.on_commit(
            lambda: send_duplicate_signal(model, new_objects, objects)
        )

    object_permission_group = dict()
    not_in_container = list()
    for old, new in zip(objects, new_objects):
        new.created = old.created
        new.duplicated = timezone.now()

        # Deal with permissions. When object is in container fix the pointer
        # to permission_group object.
        # When object is not in container new PermissionGroup proxy object must
        # be created, assigned to new object and permissions copied from old
        # object to new one.
        if (topmost_container := new.topmost_container) is not None:
            new.permission_group = topmost_container.permission_group
        else:
            not_in_container.append((new, old))
            object_permission_group[new] = PermissionGroup()

    PermissionGroup.objects.bulk_create(object_permission_group.values())
    for new, old in not_in_container:
        new.permission_group = object_permission_group[new]
        copy_permissions(old, new)
        assign_contributor_permissions(new, contributor)

    model.objects.bulk_update(
        new_objects, ["created", "duplicated", "permission_group"]
    )
    return new_objects


@transaction.atomic
def bulk_duplicate_collection(
    collections: "CollectionQuerySet",
    contributor,
    name_prefix: str = "",
) -> list["Collection"]:
    """Make a copy of given collection queryset.

    The contained objects (Entities and Data) are also copied.

    Copied objects are transformed in the following ways:

    * ``name_prefix`` ("Copy of " by default) string is prepend to names of all
      copied objects
    * all contained objects are attached to new collections and entities
    * ``input`` fields of all copied Data objects are processed and all inputs
      are replaced with their copies if they exist
    * permissions are copied from original objects
    * Data migration history is copied and linked to the new Data objects

    :param collections: A collection queryset to duplicate.
    :param contributor: A Django user that will be assigned to copied objects
        as contributor.
    :param name_prefix: A prefix that will be prepend to the name of all
        copied objects.
    """
    # Prevent circular imports.
    from resolwe.flow.models import Collection, Entity

    if not isinstance(collections, models.QuerySet):
        raise ValueError("Function only supports duplicating querysets.")

    # Make a list from collections to make sure the sort order is respected.
    # The queryset is evaluated anyway so this is not a performance issue.
    collection_list = list(collections)

    new_collections: list[Collection] = copy_objects(
        collection_list, contributor, name_prefix
    )

    collection_mapping: CollectionPKMap = dict()
    for old, new in zip(collection_list, new_collections):
        collection_mapping[old.pk] = new
        AnnotationField.add_to_collection(old, new)

    bulk_duplicate_entity(
        Entity.objects.filter(collection__in=collection_list),
        True,
        name_prefix,
        contributor,
        collection_mapping,
    )
    """"""
    # The data in the entity is already copied.
    return new_collections


@transaction.atomic
def bulk_duplicate_entity(
    entities: "EntityQuerySet",
    inherit_collection: bool = False,
    name_prefix: str = "",
    contributor=None,
    collection_mapping: "CollectionPKMap" = dict(),
) -> list["Entity"]:
    """Make a copy of given entity queryset.

    The contained Data objects are also copied.

    Copied objects are transformed in the following ways:

    * ``name_prefix`` ("Copy of " by default) string is prepend to names of all
      copied objects
    * Collections are preserved only if ``inherit_collection``is set to ``True``
    * all contained objects are attached to new collections and entities
    * ``input`` fields of all copied Data objects are processed and all inputs
      are replaced with their copies if they exist
    * permissions are copied from original objects
    * Data migration history is copied and linked to the new Data objects

    :param entities: An entity queryset to duplicate.
    :param contributor: A Django user that will be assigned to copied objects
        as contributor.
    :param inherit_collection: Indicates whether copied entities and data
        objects are added to the same collection as originals or not.
    :param name_prefix: A prefix that will be prepend to the name of all
        copied objects.
    """

    from resolwe.flow.models import Data

    entity_list = list(entities)
    entity_processor = process_entity(inherit_collection, collection_mapping)
    new_entities = copy_objects(entity_list, contributor, name_prefix, entity_processor)
    # Copy the existing annotations to the new entity and create mapping
    # between old and new entities.
    new_annotations = []
    for old_entity, new_entity in zip(
        entity_list, new_entities
    ):  # If collection is not set the annotations cannot be copied.
        if inherit_collection:
            new_annotations += old_entity.copy_annotations(new_entity, contributor)
        if old_entity.collection and new_entity.collection:
            new_entity.collection.annotation_fields.add(
                *old_entity.collection.annotation_fields.all()
            )
    AnnotationValue.objects.bulk_create(new_annotations)

    # Check permissions when collection is inherited.
    if inherit_collection:
        _check_created_objects_permissions([], new_entities, True, False, contributor)

    entity_mapping: EntityPKMap = {
        old.pk: new for old, new in zip(entity_list, new_entities)
    }

    bulk_duplicate_data(
        Data.objects.filter(entity__in=entity_list),
        inherit_collection,
        True,
        name_prefix,
        contributor,
        collection_mapping,
        entity_mapping,
    )
    return new_entities


@transaction.atomic
def bulk_duplicate_data(
    data: "DataQuerySet",
    inherit_collection: bool = False,
    inherit_entity: bool = False,
    name_prefix: str = "",
    contributor=None,
    collection_mapping: CollectionPKMap = dict(),
    entity_mapping: EntityPKMap = dict(),
) -> list["Data"]:
    """Make a copy of given data queryset.

    Copied objects are transformed in the following ways:

    * ``name_prefix`` ("Copy of " by default) string is prepend to names of all
      copied objects
    * Collection and/or entity of top-most copied objects are preserved only if
      ``inherit_collection`` and/or ``inherit_entity`` values are set to ``True``
    * ``input`` fields of all copied Data objects are processed and all inputs
      are replaced with their copies if they exist
    * permissions are copied from original objects
    * Data migration history is copied and linked to the new Data objects

    :param data: A data queryset to duplicate.
    :param contributor: A Django user that will be assigned to copied objects
        as contributor.
    :param inherit_collection: Indicates whether copied entities and data objects are
        added to the same collection as originals or not.
    :param inherit_entity: Indicates whether copied data  objects are added to the same
        collection as originals or not.
    :param name_prefix: A prefix that is prepended to the name of all copied objects.
    """
    from resolwe.flow.models import Data

    if data.exclude(status__in=[Data.STATUS_DONE, Data.STATUS_ERROR]).exists():
        raise ValidationError(
            "Data object must have done or error status to be duplicated."
        )
    data_list = list(data)
    data_processor = process_data(
        inherit_collection, inherit_entity, collection_mapping, entity_mapping
    )
    new_data = copy_objects(data_list, contributor, name_prefix, data_processor)
    _rewire_data_relations(data_list, new_data)
    _check_created_objects_permissions(
        new_data, [], inherit_collection, inherit_entity, contributor
    )
    return new_data
