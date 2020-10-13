"""Resolwe models duplicate utils."""
from copy import deepcopy

from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import IntegrityError, models, transaction
from django.utils import timezone

from guardian.models import GroupObjectPermission, UserObjectPermission

from resolwe.flow.utils import iterate_fields


def _check_permissions(objects, permission, user):
    """Check that user has permission on all given objects.

    :param objects: A list of objects to be checked.
    :type objects: list of `~django.db.models.Model`

    :param str permission: Codename of the required permission.

    :param contributor: A Django user that should have the permission.
    :type contributor: `~django.contrib.auth.models.User`

    :raises: ValidationError when user does not have all the required
        permissions.
    """
    for obj in objects:
        if not user.has_perm(permission, obj):
            raise ValidationError(
                "User doesn't have '{}' permission on {}.".format(permission, obj)
            )


def _bulk_copy_permissions(content_type, pk_mapping):
    """Copy permissions from old objects to the new ones.

    Given primary key mapping is a mapping between old object ids, from which
    permissions are copied, and new object ids, to which permissions are
    copied.

    :param content_type: Content type of objects to process.
    :type content_type: `~django.contrib.auth.models.ContentType`

    :param dict pk_mapping: Dictionary with old primary keys as keys and new
        primary keys as values.

    """
    for permission_cls in [GroupObjectPermission, UserObjectPermission]:

        permissions_qs = permission_cls.objects.filter(
            content_type=content_type, object_pk__in=pk_mapping.keys()
        )
        new_permissions = []

        for permission in permissions_qs:
            permission.pk = None
            permission.object_pk = str(pk_mapping[int(permission.object_pk)])
            new_permissions.append(permission)
        permission_cls.objects.bulk_create(new_permissions)


def _bulk_assign_contributor_permission(content_type, pks, user):
    """Assign all available permissions on given objects to given user.

    Objects are determined by the combination of the content type and a
    list of primary keys.

    :param content_type: Content type of objects to process.
    :type content_type: `~django.contrib.auth.models.ContentType`

    :param list pks: List of primary keys of objects to process.

    :param user: A Django user to which permissions should be assigned.
    :type contributor: `~django.contrib.auth.models.User`

    """
    permissions = Permission.objects.filter(content_type=content_type)

    new_permissions = []
    for permission in permissions:
        for pk in pks:
            new_permissions.append(
                UserObjectPermission(
                    content_type=content_type,
                    object_pk=pk,
                    permission=permission,
                    user=user,
                )
            )
    UserObjectPermission.objects.bulk_create(new_permissions, ignore_conflicts=True)


def _rewire_foreign_key(objects, target, pk_mapping):
    """Make a copy of given objects and rewire references to target objects.

    All fields that points to an object of ``target`` type are replaced with
    the new objects according to the provided mapping. The list of new objects
    is returned.

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
    obj_class = objects[0].__class__
    if any(obj.__class__ != obj_class for obj in objects):
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
            field_name = "{}_id".format(field_name)
            value = getattr(obj, field_name)
            if value and value in pk_mapping:
                setattr(new_obj, field_name, pk_mapping[value])

        new_obj.pk = None
        new_objects.append(new_obj)

    return obj_class.objects.bulk_create(new_objects)


def _rewire_inputs(data, pk_mapping):
    """Rewire inputs of provided data objects.

    References to input data objects in ``input`` field of the Data objects are
    chenged with references of their copies according to the provided mapping.
    If primary key is not in the mapping, original value is kept.

    :param list data: A list of Data objects to process.

    :param dict pk_mapping: A dict where keys are primary keys of original Data
        objects and values are primary keys of their copies.

    """
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


def process_entity(inherit_collection, collection_mapping):
    """Return a function that process a duplicated Entity before saving.

    :param bool inherit_collection: Indicates whether the collection should
        be inherited or not.

    :param dict collection_mapping: A dict where keys are primary keys of
        original collections and values are primary keys of their copies.

    """

    def processor(entity):
        """Set collection of given Entity.

        Collection is to ``None`` if ``inherit_collection`` value is set to
        ``False``, or mapped according to ``collection_mapping`` otherwise (or
        left as is if value is not in the mapping).

        :param entity: An entity to be processed.
        :type entity: `~resolwe.flow.models.Entity`

        :rtype: `~resolwe.flow.models.Entity`

        """
        if inherit_collection and entity.collection:
            if entity.collection.id in collection_mapping:
                entity.collection = collection_mapping[entity.collection.pk]
        else:
            entity.collection = None

        return entity

    return processor


def process_data(
    inherit_collection, inherit_entity, collection_mapping, entity_mapping
):
    """Return a function that process a duplicated Data object before saving.

    :param bool inherit_collection: Indicates whether the collection should
        be inherited or not.

    :param bool inherit_entity: Indicates whether the entity should be
        inherited or not.

    :param dict collection_mapping: A dict where keys are primary keys of
        original collections and values are primary keys of their copies.

    :param dict entity_mapping: A dict where keys are primary keys of original
        entities and values are primary keys of their copies.

    """

    def processor(datum):
        """Set collection and entity of given Data object.

        Collection and entity are set to ``None`` if ``inherit_collection``
        and ``inherit_entity`` values are set to ``False``, respectively, and
        mapped according to ``collection_mapping`` and ``entity_mapping``
        otherwise (or left as is if value is not in the mapping).

        :param datum: An Dat object to be processed.
        :type datum: `~resolwe.flow.models.Data`

        :rtype: `~resolwe.flow.models.Data`

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


def copy_objects(objects, contributor, name_prefix, obj_processor=None):
    """Make a copy of given queryset.

    Shallow copy given queryset and set contributor to the given value,
    prepend name with the prefix, set slug to a unique value, and set
    ``duplicated`` date to the current time. Special attention is paid
    to keep the ``created`` date to its original value.

    If ``obj_processor`` function is given, each object is passed to it
    and the return value is used instead of it.

    :param objects: A queryset to be copied.
    :type objects: `~resolwe.flow.models.base.BaseQuerySet`

    :param contributor: A Django user that will be assigned to copied objects
        as contributor.
    :type contributor: `~django.contrib.auth.models.User`

    :param str name_prefix: A prefix that will be prepend to the name of copied
        objects.

    """
    first = objects.first()
    if not first:
        return objects

    name_max_length = first._meta.get_field("name").max_length
    content_type = ContentType.objects.get_for_model(first)
    model = first._meta.model

    new_objects = []
    for obj in objects:
        new_obj = deepcopy(obj)
        new_obj.pk = None
        new_obj.slug = None
        new_obj.contributor = contributor
        new_obj.name = "{} {}".format(name_prefix, obj.name)

        if len(new_obj.name) > name_max_length:
            new_obj.name = "{}...".format(new_obj.name[: name_max_length - 3])

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
            # Call the parent method to skip pre-processing and validation.
            models.Model.save(obj)

    for old, new in zip(objects, new_objects):
        new.created = old.created
        new.duplicated = timezone.now()

    model.objects.bulk_update(new_objects, ["created", "duplicated"])

    pk_mapping = {old.pk: new.pk for old, new in zip(objects, new_objects)}
    _bulk_copy_permissions(content_type, pk_mapping)
    _bulk_assign_contributor_permission(content_type, pk_mapping.values(), contributor)

    return new_objects


@transaction.atomic
def bulk_duplicate(
    collections=None,
    entities=None,
    data=None,
    contributor=None,
    inherit_collection=False,
    inherit_entity=False,
    name_prefix=None,
):
    """Make a copy of given collection, entity or data queryset.

    Exactly one of ``collections``, ``entities`` and ``data`` parameters should
    be passed to the function and should respectively represent a queryset of
    Collection, Entity and Data objects to be copied.

    When copying Collections or Entities, also the contained objects (Entities
    and Data) are also copied.

    Copied objects are transformed in the following ways:

    * ``name_prefix`` ("Copy of " by default) string is prepend to names of all
      copied objects
    * Collection and/or entity of top-most copied objects are preserved only if
      ``inherit_collection`` and/or ``inherit_entity`` values are set to
      ``True``
    * all contained objects are attached to new collections and entities
    * ``input`` fields of all copied Data objects are processed and all inputs
      are replaced with their copies if they exist
    * permissions are copied from original objects
    * Data migration history is copied and linked to the new Data objects

    :param collections: A collection queryset to duplicate.
    :type collections: `~resolwe.flow.models.collection.CollectionQuerySet`

    :param entities: An entity queryset to duplicate.
    :type entity: `~resolwe.flow.models.entity.EntityQuerySet`

    :param data: A data queryset to duplicate.
    :type data: `~resolwe.flow.models.data.DataQuerySet`

    :param contributor: A Django user that will be assigned to copied objects
        as contributor.
    :type contributor: `~django.contrib.auth.models.User`

    :param bool inherit_collection: Indicates whether copied entities and data
        objects are added to the same collection as originals or not.

    :param bool inherit_entity: Indicates whether copied data  objects are
        added to the same collection as originals or not.

    :param str name_prefix: A prefix that will be prepend to the name of all
        copied objects.

    :rtype: `~resolwe.flow.models.collection.CollectionQuerySet` or
        `~resolwe.flow.models.entity.EntityQuerySet` or
        `~resolwe.flow.models.data.DataQuerySet`

    """
    # Prevent circular import.
    from resolwe.flow.models import (
        Collection,
        Data,
        DataDependency,
        DataMigrationHistory,
        Entity,
    )

    name_prefix = name_prefix or "Copy of"

    if not contributor:
        raise ValueError("Contributor must be given.")

    specified = [model for model in [collections, entities, data] if model is not None]
    if len(specified) != 1:
        raise ValueError(
            "Exactly one of 'collections', 'entites', and 'data' attributes must be given."
        )
    if not isinstance(specified[0], models.QuerySet):
        raise ValueError("Function only supports duplcating querysets.")

    collection_mapping, entity_mapping = {}, {}

    if collections is not None:
        new_collections = copy_objects(collections, contributor, name_prefix)

        collection_mapping = {
            old.pk: new for old, new in zip(collections, new_collections)
        }

        entities = Entity.objects.filter(collection__in=collections)
        data = Data.objects.filter(collection__in=collections)

        inherit_collection = True

    if entities is not None:
        entity_processor = process_entity(inherit_collection, collection_mapping)
        new_entities = copy_objects(
            entities, contributor, name_prefix, entity_processor
        )

        entity_mapping = {old.pk: new for old, new in zip(entities, new_entities)}

        # Entity data is also included in the collection, so we don't need to set it
        # here if it was already set (i.e. if we are duplicating a collection).
        if data is None:
            data = Data.objects.filter(entity__in=entities)

        inherit_entity = True

    if data.exclude(status__in=[Data.STATUS_DONE, Data.STATUS_ERROR]).exists():
        raise ValidationError(
            "Data object must have done or error status to be duplicated."
        )

    data_processor = process_data(
        inherit_collection, inherit_entity, collection_mapping, entity_mapping
    )
    new_data = copy_objects(data, contributor, name_prefix, data_processor)

    data_pks = data.values_list("pk")
    pk_mapping = {old.pk: new.pk for old, new in zip(data, new_data)}

    _rewire_inputs(new_data, pk_mapping)
    Data.objects.bulk_update(new_data, ["input"])

    # Add m2m storage relations.
    through_model = Data.storages.through
    storage_relations = through_model.objects.filter(data__in=data_pks)
    _rewire_foreign_key(storage_relations, Data, pk_mapping)

    # Copy data dependencies.
    # We only inherit parent dependencies to keep track of the Data object's
    # source. Child dependencies reflect later actions that don't directly
    # depend on the current object, so copying them would only make unnecessary
    # noise. Duplicate dependencies are not copied to keep a simple chain with
    # links only to direct predecessors.
    data_dependencies = DataDependency.objects.filter(
        kind__in=[DataDependency.KIND_IO, DataDependency.KIND_SUBPROCESS],
        child__in=data_pks,
    )
    _rewire_foreign_key(data_dependencies, Data, pk_mapping)

    # Add duplicate dependencies.
    DataDependency.objects.bulk_create(
        DataDependency(parent=old, child=new, kind=DataDependency.KIND_DUPLICATE)
        for old, new in zip(data, new_data)
    )

    # Copy migration history. Ordering is needed to keep the order of migrations.
    migration_history = DataMigrationHistory.objects.filter(data__in=data_pks).order_by(
        "pk"
    )
    _rewire_foreign_key(migration_history, Data, pk_mapping)

    if inherit_collection:
        data_collections = Collection.objects.filter(data__in=new_data)
        _check_permissions(data_collections, "edit_collection", contributor)
        if entities is not None:
            entity_collections = Collection.objects.filter(entity__in=new_entities)
            _check_permissions(entity_collections, "edit_collection", contributor)

    if inherit_entity:
        data_entities = Entity.objects.filter(data__in=new_data)
        _check_permissions(data_entities, "edit_entity", contributor)

    if collections is not None:
        return new_collections
    elif entities is not None:
        return new_entities
    return new_data
