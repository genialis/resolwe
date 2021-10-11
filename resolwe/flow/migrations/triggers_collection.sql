-- Trigger after insert/update Collection object.
CREATE OR REPLACE FUNCTION generate_resolwe_collection_search(collection flow_collection)
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
            AND permissions_permissionmodel.permission_group_id::int=collection.permission_group_id;

        --JOIN guardian_userobjectpermission ON auth_user.id=guardian_userobjectpermission.user_id
        --WHERE
        --    content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='collection')
        --    AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_collection')
        --    AND object_pk::int=collection.id
        --GROUP BY object_pk;

        SELECT
            username usernames, first_name first_names, last_name last_names
        INTO contributor
        FROM auth_user
        WHERE id = collection.contributor_id;

        SELECT COALESCE(flatten_descriptor_values(collection.descriptor), '') INTO flat_descriptor;

        SELECT
            -- Collection name.
            setweight(to_tsvector('simple', collection.name), 'A') ||
            setweight(to_tsvector('simple', get_characters(collection.name)), 'B') ||
            setweight(to_tsvector('simple', get_numbers(collection.name)), 'B') ||
            -- Collection description.
            setweight(to_tsvector('simple', collection.description), 'B') ||
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
            -- Collection tags.
            setweight(to_tsvector('simple', array_to_string(collection.tags, ' ')), 'B') ||
            -- Collection descriptor.
            setweight(to_tsvector('simple', flat_descriptor), 'C') ||
            setweight(to_tsvector('simple', get_characters(flat_descriptor)), 'D') ||
            setweight(to_tsvector('simple', get_numbers(flat_descriptor)), 'D')

        INTO search;

        RETURN search;
    END;
    $$;

CREATE OR REPLACE FUNCTION collection_biut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        SELECT generate_resolwe_collection_search(NEW) INTO NEW.search;

        IF NEW.search IS NULL THEN
            RAISE WARNING 'Search index for collection (id: %) is NULL.', NEW.id;
        END IF;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER collection_biut
    BEFORE INSERT OR UPDATE
    ON flow_collection
    FOR EACH ROW EXECUTE PROCEDURE collection_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_collection(perm_users permissions_permissionmodel_users)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        permission_group int;
        permission int;
        collection_id int;
    BEGIN
        SELECT 
            permissions_permissionmodel.permission, permissions_permissionmodel.permission_group_id
            FROM permissions_permissionmodel
            WHERE id = perm_users.permissionmodel_id
            INTO permission, permission_group;

        IF permission = 8 THEN
            -- Set the search field to NULL to trigger data_biut.
            FOR collection_id in 
                SELECT id FROM flow_collection WHERE flow_collection.permission_group_id = permission_group
            LOOP
                UPDATE flow_collection SET search=NULL WHERE id=collection_id;
            END LOOP;
        END IF;
    END;
    $$;

CREATE OR REPLACE FUNCTION userpermission_collection_aiut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_collection(NEW);
        RETURN NEW;
    END;
    $$;

CREATE TRIGGER userpermission_collection_aiut
    AFTER INSERT OR UPDATE
    -- ON guardian_userobjectpermission
    ON permissions_permissionmodel_users
    FOR EACH ROW EXECUTE PROCEDURE userpermission_collection_aiut();

CREATE OR REPLACE FUNCTION userpermission_collection_adt()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_collection(OLD);
        RETURN OLD;
    END;
    $$;

CREATE TRIGGER userpermission_collection_adt
    AFTER DELETE
    -- ON guardian_userobjectpermission
    ON permissions_permissionmodel_users
    FOR EACH ROW EXECUTE PROCEDURE userpermission_collection_adt();


-- Trigger after update contributor.
CREATE OR REPLACE FUNCTION collection_contributor_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Set the search field to NULL to trigger collection_biut.
        UPDATE flow_collection SET search=NULL WHERE flow_collection.contributor_id=NEW.id;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER collection_contributor_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE collection_contributor_aut();


-- Trigger after update owner.
-- TODO!
-- CREATE OR REPLACE FUNCTION collection_owner_aut()
--     RETURNS TRIGGER
--     LANGUAGE plpgsql
--     AS $$
--     BEGIN
--         WITH owner_permission AS (
--             SELECT object_pk::int collection_id
--             FROM guardian_userobjectpermission
--             WHERE
--                 user_id=NEW.id
--                 AND content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='collection')
--                 AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_collection')
--         )
--         -- Set the search field to NULL to trigger collection_biut.
--         UPDATE flow_collection collection
--         SET search=NULL
--         FROM owner_permission perm
--         WHERE collection.id=perm.collection_id;

--         RETURN NEW;
--     END;
--     $$;

-- CREATE TRIGGER collection_owner_aut
--     AFTER UPDATE
--     ON auth_user
--     FOR EACH ROW EXECUTE PROCEDURE collection_owner_aut();
