-- Trigger after insert/update Entity object.
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

        RETURN search;
    END;
    $$;

CREATE OR REPLACE FUNCTION entity_biut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        SELECT generate_resolwe_entity_search(NEW) INTO NEW.search;

        IF NEW.search IS NULL THEN
            RAISE WARNING 'Search index for entity (id: %) is NULL.', NEW.id;
        END IF;

        RETURN NEW;
    END;
    $$;

CREATE OR REPLACE TRIGGER entity_biut
    BEFORE INSERT OR UPDATE
    ON flow_entity
    FOR EACH ROW EXECUTE PROCEDURE entity_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_entity(user_permission permissions_permissionmodel)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        entity_id int;
    BEGIN
        IF user_permission.value=8 THEN
            -- Set the search field to NULL to trigger data_biut.
            FOR entity_id in 
                SELECT id FROM flow_entity WHERE flow_entity.permission_group_id = user_permission.permission_group_id
            LOOP
                UPDATE flow_entity SET search=NULL WHERE id=entity_id;
            END LOOP;
        END IF;
    END;
    $$;

CREATE OR REPLACE FUNCTION userpermission_entity_aiut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_entity(NEW);
        RETURN NEW;
    END;
    $$;

CREATE OR REPLACE TRIGGER userpermission_entity_aiut
    AFTER INSERT OR UPDATE
    ON permissions_permissionmodel
    FOR EACH ROW EXECUTE PROCEDURE userpermission_entity_aiut();

CREATE OR REPLACE FUNCTION userpermission_entity_adt()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_entity(OLD);
        RETURN OLD;
    END;
    $$;

CREATE OR REPLACE TRIGGER userpermission_entity_adt
    AFTER DELETE
    ON permissions_permissionmodel
    FOR EACH ROW EXECUTE PROCEDURE userpermission_entity_adt();


-- Trigger after update contributor.
CREATE OR REPLACE FUNCTION entity_contributor_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity SET search=NULL WHERE flow_entity.contributor_id=NEW.id;

        RETURN NEW;
    END;
    $$;

CREATE OR REPLACE TRIGGER entity_contributor_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE entity_contributor_aut();


-- Trigger after update owner.
CREATE OR REPLACE FUNCTION entity_owner_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        WITH permission_group AS (
            SELECT permission_group_id::int
            FROM permissions_permissionmodel
            WHERE
                permissions_permissionmodel.user_id=NEW.id AND
                permissions_permissionmodel.value=8
        )

        -- Set the search field to NULL to trigger entity_biut.
        UPDATE flow_entity entity
        SET search=NULL
        FROM permission_group perm_group
        WHERE entity.permission_group_id=perm_group.permission_group_id;

        RETURN NEW;
    END;
    $$;

CREATE OR REPLACE TRIGGER entity_owner_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE entity_owner_aut();
