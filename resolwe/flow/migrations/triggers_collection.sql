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
        JOIN permissions_permissionmodel ON auth_user.id=permissions_permissionmodel.user_id
        WHERE
            permissions_permissionmodel.value=8
            AND permissions_permissionmodel.permission_group_id::int=collection.permission_group_id;

        SELECT
            username usernames, first_name first_names, last_name last_names
        INTO contributor
        FROM auth_user
        WHERE id = collection.contributor_id;

        SELECT COALESCE(flatten_descriptor_values(collection.descriptor), '') INTO flat_descriptor;

        SELECT
            -- Collection name.
            setweight(to_tsvector('simple_unaccent', collection.name), 'A') ||
            setweight(to_tsvector('simple_unaccent', get_characters(collection.name)), 'B') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(collection.name)), 'B') ||
            -- Collection description.
            setweight(to_tsvector('simple_unaccent', collection.description), 'B') ||
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
            -- Collection tags.
            setweight(to_tsvector('simple_unaccent', array_to_string(collection.tags, ' ')), 'B') ||
            -- Collection descriptor.
            setweight(to_tsvector('simple_unaccent', flat_descriptor), 'C') ||
            setweight(to_tsvector('simple_unaccent', get_characters(flat_descriptor)), 'D') ||
            setweight(to_tsvector('simple_unaccent', get_numbers(flat_descriptor)), 'D')

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

CREATE OR REPLACE TRIGGER collection_biut
    BEFORE INSERT OR UPDATE
    ON flow_collection
    FOR EACH ROW EXECUTE PROCEDURE collection_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_collection(user_permission permissions_permissionmodel)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        collection_id int;
    BEGIN
        IF user_permission.value=8 THEN
            -- Set the search field to NULL to trigger data_biut.
            FOR collection_id in 
                SELECT id FROM flow_collection WHERE flow_collection.permission_group_id = user_permission.permission_group_id
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

CREATE OR REPLACE TRIGGER userpermission_collection_aiut
    AFTER INSERT OR UPDATE
    ON permissions_permissionmodel
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

CREATE OR REPLACE TRIGGER userpermission_collection_adt
    AFTER DELETE
    -- ON guardian_userobjectpermission
    ON permissions_permissionmodel
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

CREATE OR REPLACE TRIGGER collection_contributor_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE collection_contributor_aut();


-- Trigger after update owner.
CREATE OR REPLACE FUNCTION collection_owner_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        WITH owner_permission AS (
            SELECT permission_group_id::int
            FROM permissions_permissionmodel
            WHERE
                permissions_permissionmodel.user_id=NEW.id AND
                permissions_permissionmodel.value=8
        )
        -- Set the search field to NULL to trigger collection_biut.
        UPDATE flow_collection collection
        SET search=NULL
        FROM owner_permission perm
        WHERE collection.permission_group_id=perm.permission_group_id;

        RETURN NEW;
    END;
    $$;

CREATE OR REPLACE TRIGGER collection_owner_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE collection_owner_aut();
