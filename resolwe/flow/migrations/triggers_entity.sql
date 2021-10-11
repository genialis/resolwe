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
        JOIN permissions_permissionmodel_users ON auth_user.id=permissions_permissionmodel_users.user_id
        JOIN permissions_permissionmodel ON permissions_permissionmodel_users.permissionmodel_id=permissions_permissionmodel.id
        WHERE
            permissions_permissionmodel.permission=8
            AND permissions_permissionmodel.permission_group_id::int=entity.permission_group_id;

        SELECT
            username usernames, first_name first_names, last_name last_names
        INTO contributor
        FROM auth_user
        WHERE id = entity.contributor_id;

        SELECT COALESCE(flatten_descriptor_values(entity.descriptor), '') INTO flat_descriptor;

        SELECT
            -- Entity name.
            setweight(to_tsvector('simple', entity.name), 'A') ||
            setweight(to_tsvector('simple', get_characters(entity.name)), 'B') ||
            setweight(to_tsvector('simple', get_numbers(entity.name)), 'B') ||
            -- Collection description.
            setweight(to_tsvector('simple', entity.description), 'B') ||
            -- Contributor username.
            setweight(to_tsvector('simple', contributor.usernames), 'B') ||
            setweight(to_tsvector('simple', get_characters(contributor.usernames)), 'C') ||
            setweight(to_tsvector('simple', get_numbers(contributor.usernames)), 'C') ||
            -- Contributor first name.
            setweight(to_tsvector('simple', contributor.first_names), 'B') ||
            -- Contributor last name.
            setweight(to_tsvector('simple', contributor.last_names), 'B') ||
            -- Owners usernames. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple', COALESCE(owners.usernames, '')), 'B') ||
            setweight(to_tsvector('simple', get_characters(owners.usernames)), 'C') ||
            setweight(to_tsvector('simple', get_numbers(owners.usernames)), 'C') ||
            -- Owners first names. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple', COALESCE(owners.first_names, '')), 'B') ||
            -- Owners last names. There is no guarantee that it is not NULL.
            setweight(to_tsvector('simple', COALESCE(owners.last_names, '')), 'B') ||
            -- Entity tags.
            setweight(to_tsvector('simple', array_to_string(entity.tags, ' ')), 'B') ||
            -- Entity descriptor.
            setweight(to_tsvector('simple', flat_descriptor), 'C') ||
            setweight(to_tsvector('simple', get_characters(flat_descriptor)), 'D') ||
            setweight(to_tsvector('simple', get_numbers(flat_descriptor)), 'D')

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

CREATE TRIGGER entity_biut
    BEFORE INSERT OR UPDATE
    ON flow_entity
    FOR EACH ROW EXECUTE PROCEDURE entity_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_entity(perm_users permissions_permissionmodel_users)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        permission_group int;
        permission int;
        entity_id int;
    BEGIN
        SELECT 
            permissions_permissionmodel.permission, permissions_permissionmodel.permission_group_id
            FROM permissions_permissionmodel
            WHERE id = perm_users.permissionmodel_id
            INTO permission, permission_group;

        IF permission = 8 THEN
            -- Set the search field to NULL to trigger data_biut.
            FOR entity_id in 
                SELECT id FROM flow_entity WHERE flow_entity.permission_group_id = permission_group
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

CREATE TRIGGER userpermission_entity_aiut
    AFTER INSERT OR UPDATE
    ON permissions_permissionmodel_users
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

CREATE TRIGGER userpermission_entity_adt
    AFTER DELETE
    ON permissions_permissionmodel_users
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

CREATE TRIGGER entity_contributor_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE entity_contributor_aut();


-- Trigger after update owner.
-- CREATE OR REPLACE FUNCTION entity_owner_aut()
--     RETURNS TRIGGER
--     LANGUAGE plpgsql
--     AS $$
--     BEGIN
--         WITH permission_group AS (
--             SELECT id
--             FROM permission_permissiongroup
--             WHERE
                
--                 AND content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='entity')
--                 AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_entity')

--         JOIN permissions_permissionmodel_users ON auth_user.id=permissions_permissionmodel_users.user_id
--         JOIN permissions_permissionmodel ON permissions_permissionmodel_users.permissionmodel_id=permissions_permissionmodel.id
--         WHERE
--             permissions_permissionmodel.permission=8 and auth_user.id=NEW.id

--         )
--         -- Set the search field to NULL to trigger entity_biut.
--         UPDATE flow_entity entity
--         SET search=NULL
--         FROM permission_group perm_group
--         WHERE entity.permission_group_id=perm_group.id;

--         RETURN NEW;
--     END;
--     $$;

-- CREATE TRIGGER entity_owner_aut
--     AFTER UPDATE
--     ON auth_user
--     FOR EACH ROW EXECUTE PROCEDURE entity_owner_aut();
