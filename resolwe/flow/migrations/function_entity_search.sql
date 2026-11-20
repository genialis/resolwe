-- Compute the full-text search vector of an entity.
--
-- The function is in its own file so a migration that changes it can replace it
-- without recreating the entity triggers: CREATE OR REPLACE FUNCTION takes no
-- table lock, while CREATE OR REPLACE TRIGGER locks the tables the triggers are
-- on until the migration commits.
CREATE OR REPLACE FUNCTION generate_resolwe_entity_search(entity flow_entity)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        owners users_result;
        contributor users_result;
        flat_descriptor text;
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
            setweight(to_tsvector('simple_unaccent', array_to_string(entity.tags, ' ')), 'B')
        INTO search;

        -- Entity annotation values. The function is defined in
        -- function_entity_annotation_search.sql and does not exist yet when older
        -- migrations load this file.
        IF to_regprocedure('generate_resolwe_entity_annotation_search(flow_entity)') IS NOT NULL THEN
            search := search || generate_resolwe_entity_annotation_search(entity);
        END IF;

        RETURN search;
    END;
    $$;
