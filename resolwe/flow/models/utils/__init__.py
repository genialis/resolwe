"""Resolwe models utils."""

from resolwe.flow.utils import iterate_fields

from .duplicate import (  # noqa: F401
    bulk_duplicate_collection,
    bulk_duplicate_data,
    bulk_duplicate_entity,
)
from .hydrate import (  # noqa: F401
    hydrate_input_references,
    hydrate_input_uploads,
    hydrate_size,
)
from .reference import referenced_files  # noqa: F401
from .render import (  # noqa: F401
    fill_with_defaults,
    json_path_components,
    render_descriptor,
    render_template,
)
from .validation import (  # noqa: F401
    DirtyError,
    validate_data_object,
    validate_process_types,
    validate_schema,
    validation_schema,
)


def get_collection_of_input_entities(data):
    """Get collection that contains all "entity inputs" of a given data.

    With "entity input", one refers to the inputs that are part of an entity.
    """
    # Prevent circular imports:
    from resolwe.flow.models import Collection

    data_ids = set()

    for field_schema, fields in iterate_fields(data.input, data.process.input_schema):
        name = field_schema["name"]
        value = fields[name]
        if "type" not in field_schema:
            continue

        if field_schema["type"].startswith("data:"):
            value = [value]
        elif not field_schema["type"].startswith("list:data:"):
            continue

        data_ids.update([val for val in value if val is not None])

    collections = Collection.objects.filter(
        data__in=list(data_ids),
        data__entity__isnull=False,
    ).distinct()

    if collections.count() != 1:
        raise ValueError(
            "Entity inputs should be part of exactly one collection. (not {})".format(
                len(collections)
            )
        )

    return collections.first()


def serialize_collection_relations(collection):
    """Serialize all relations of a collection."""
    # Prevent circular imports:
    from resolwe.flow.models import Relation, RelationPartition

    serialized_relations = []
    for relation in Relation.objects.filter(collection=collection):
        serialized_relations.append(
            {
                "relation_id": relation.id,
                "relation_type_name": relation.type.name,
                "relation_type_ordered": relation.type.ordered,
                "category": relation.category,
                "unit": relation.unit,
                "partitions": list(
                    RelationPartition.objects.filter(relation=relation).values()
                ),
            }
        )

    return serialized_relations
