-- Index the annotation values of entities in their full-text search vector.
--
-- This file replaces generate_resolwe_entity_search from triggers_entity.sql and must
-- be loaded after it. It is separate because older migrations load
-- triggers_entity.sql and recompute the entity search vectors before the
-- flow_annotationvalue table and its deleted column exist.

CREATE OR REPLACE FUNCTION generate_resolwe_entity_search(entity flow_entity)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        owners users_result;
        contributor users_result;
        flat_annotations text;
        search tsvector;
    BEGIN
        SELECT
            array_to_string(array_agg(username), ' ') AS usernames,
            array_to_string(array_remove(array_agg(first_name), ''), ' ') AS first_names,
            array_to_string(array_remove(array_agg(last_name), ''), ' ') AS last_names
        INTO owners
        FROM auth_user
        JOIN permissions_permissionmodel ON auth_user.id=permissions_permissionmodel.user_id
        WHERE
            permissions_permissionmodel.value=8
            AND permissions_permissionmodel.permission_group_id::int=entity.permission_group_id;

        SELECT
            username usernames, first_name first_names, last_name last_names
        INTO contributor
        FROM auth_user
        WHERE id = entity.contributor_id;

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

        SELECT
            -- Entity name.
            setweight(to_tsvector('simple_unaccent', entity.name), 'A') ||
            setweight(to_tsvector('simple_unaccent', get_characters(entity.name)), 'B') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(entity.name)), 'B') ||
            -- Collection description.
            setweight(to_tsvector('simple_unaccent', entity.description), 'B') ||
            -- Contributor username.
            setweight(to_tsvector('simple_unaccent', contributor.usernames), 'B') ||
            setweight(to_tsvector('simple_unaccent', get_characters(contributor.usernames)), 'C') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(contributor.usernames)), 'C') ||
            -- Contributor first name.
            setweight(to_tsvector('simple_unaccent', contributor.first_names), 'B') ||
            -- Contributor last name.
            setweight(to_tsvector('simple_unaccent', contributor.last_names), 'B') ||
            -- Owners usernames. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple_unaccent', COALESCE(owners.usernames, '')), 'B') ||
            setweight(to_tsvector('simple_unaccent', get_characters(owners.usernames)), 'C') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(owners.usernames)), 'C') ||
            -- Owners first names. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple_unaccent', COALESCE(owners.first_names, '')), 'B') ||
            -- Owners last names. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple_unaccent', COALESCE(owners.last_names, '')), 'B') ||
            -- Entity tags.
            setweight(to_tsvector('simple_unaccent', array_to_string(entity.tags, ' ')), 'B') ||
            -- Entity annotation values.
            setweight(to_tsvector('simple_unaccent', flat_annotations), 'C') ||
            setweight(to_tsvector('simple_unaccent', get_characters(flat_annotations)), 'D') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(flat_annotations)), 'D')
        INTO search;

        RETURN search;
    END;
    $$;


-- Recompute the search vector of entities whose annotation values change. The
-- triggers run once per statement, so a bulk change recomputes the search vector of
-- each affected entity once.
CREATE OR REPLACE FUNCTION annotationvalue_entity_recompute(entity_ids bigint[])
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Lock the entities in id order, so that statements of concurrent transactions
        -- lock their entities in the same order.
        PERFORM 1 FROM flow_entity WHERE id = ANY(entity_ids) ORDER BY id FOR NO KEY UPDATE;
        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity SET search=NULL WHERE id = ANY(entity_ids);
    END;
    $$;

CREATE OR REPLACE FUNCTION annotationvalue_entity_ait()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        PERFORM annotationvalue_entity_recompute(ARRAY(SELECT entity_id FROM new_annotation_values));
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
        PERFORM annotationvalue_entity_recompute(ARRAY(
            SELECT entity_id FROM old_annotation_values
            UNION
            SELECT entity_id FROM new_annotation_values
        ));
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
        PERFORM annotationvalue_entity_recompute(ARRAY(SELECT entity_id FROM old_annotation_values));
        RETURN NULL;
    END;
    $$;

CREATE OR REPLACE TRIGGER annotationvalue_entity_adt
    AFTER DELETE
    ON flow_annotationvalue
    REFERENCING OLD TABLE AS old_annotation_values
    FOR EACH STATEMENT EXECUTE PROCEDURE annotationvalue_entity_adt();
