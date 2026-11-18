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
