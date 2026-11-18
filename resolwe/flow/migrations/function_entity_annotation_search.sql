-- Index the annotation values of entities in their full-text search vector.
--
-- generate_resolwe_entity_search in triggers_entity.sql appends this part when the
-- function exists. It is in a separate file because older migrations load
-- triggers_entity.sql before the flow_annotationvalue table exists. Migration 0039
-- creates the function and its reverse drops it.
CREATE OR REPLACE FUNCTION generate_resolwe_entity_annotation_search(entity flow_entity)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        flat_annotations text;
    BEGIN
        -- Labels of the live annotation values, and their values when they differ
        -- (vocabulary values).
        SELECT COALESCE(
            string_agg(
                CASE
                    WHEN annotation._value->>'value' IS DISTINCT FROM annotation._value->>'label'
                    THEN concat_ws(' ', annotation._value->>'label', annotation._value->>'value')
                    ELSE annotation._value->>'label'
                END,
                ' '
            ),
            ''
        )
        INTO flat_annotations
        FROM flow_annotationvalue annotation
        WHERE annotation.entity_id = entity.id AND annotation.deleted = false;

        RETURN
            setweight(to_tsvector('simple_unaccent', flat_annotations), 'C') ||
            setweight(to_tsvector('simple_unaccent', get_characters(flat_annotations)), 'D') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(flat_annotations)), 'D');
    END;
    $$;
