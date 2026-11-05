-- Recompute the search vector of entities whose annotation values change.
-- generate_resolwe_entity_search in triggers_entity.sql reads the values. This file
-- is separate because older migrations load triggers_entity.sql before the
-- flow_annotationvalue table exists. The triggers run once per statement, so a bulk
-- change recomputes the search vector of each affected entity once.

CREATE OR REPLACE FUNCTION annotationvalue_entity_ait()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity SET search=NULL
        WHERE id IN (SELECT entity_id FROM new_annotation_values);
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
        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity SET search=NULL
        WHERE id IN (
            SELECT entity_id FROM old_annotation_values
            UNION
            SELECT entity_id FROM new_annotation_values
        );
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
        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity SET search=NULL
        WHERE id IN (SELECT entity_id FROM old_annotation_values);
        RETURN NULL;
    END;
    $$;

CREATE OR REPLACE TRIGGER annotationvalue_entity_adt
    AFTER DELETE
    ON flow_annotationvalue
    REFERENCING OLD TABLE AS old_annotation_values
    FOR EACH STATEMENT EXECUTE PROCEDURE annotationvalue_entity_adt();
