-- Index the annotation values of entities in their full-text search vector.
--
-- generate_resolwe_entity_search in triggers_entity.sql appends this part when the
-- function exists. It is in a separate file because older migrations load
-- triggers_entity.sql before the flow_annotationvalue table exists.
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


-- Queue the entities whose annotation values change. The listener rebuilds their
-- search vectors outside the writing transaction (rebuild_entity_search), so the
-- writers never lock entity rows. The triggers run once per statement.
CREATE OR REPLACE FUNCTION annotationvalue_entity_ait()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        INSERT INTO flow_entitysearchqueue (entity_id, created)
        SELECT DISTINCT entity_id, now() FROM new_annotation_values;
        RETURN NULL;
    END;
    $$;

CREATE OR REPLACE TRIGGER annotationvalue_entity_ait
    AFTER INSERT
    ON flow_annotationvalue
    REFERENCING NEW TABLE AS new_annotation_values
    FOR EACH STATEMENT EXECUTE PROCEDURE annotationvalue_entity_ait();

CREATE OR REPLACE FUNCTION annotationvalue_entity_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Only changes of the columns the search vector depends on.
        WITH changed AS (
            SELECT old_value.entity_id AS old_entity_id, new_value.entity_id AS new_entity_id
            FROM old_annotation_values old_value
            JOIN new_annotation_values new_value ON new_value.id = old_value.id
            WHERE
                old_value.entity_id IS DISTINCT FROM new_value.entity_id
                OR old_value.deleted IS DISTINCT FROM new_value.deleted
                OR old_value._value IS DISTINCT FROM new_value._value
        )
        INSERT INTO flow_entitysearchqueue (entity_id, created)
        SELECT entity_id, now() FROM (
            SELECT old_entity_id AS entity_id FROM changed
            UNION
            SELECT new_entity_id FROM changed
        ) AS entities;
        RETURN NULL;
    END;
    $$;

CREATE OR REPLACE TRIGGER annotationvalue_entity_aut
    AFTER UPDATE
    ON flow_annotationvalue
    REFERENCING OLD TABLE AS old_annotation_values NEW TABLE AS new_annotation_values
    FOR EACH STATEMENT EXECUTE PROCEDURE annotationvalue_entity_aut();

CREATE OR REPLACE FUNCTION annotationvalue_entity_adt()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        INSERT INTO flow_entitysearchqueue (entity_id, created)
        SELECT DISTINCT entity_id, now() FROM old_annotation_values;
        RETURN NULL;
    END;
    $$;

CREATE OR REPLACE TRIGGER annotationvalue_entity_adt
    AFTER DELETE
    ON flow_annotationvalue
    REFERENCING OLD TABLE AS old_annotation_values
    FOR EACH STATEMENT EXECUTE PROCEDURE annotationvalue_entity_adt();
