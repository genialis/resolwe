-- Trigger after insert/update Data object.
CREATE TYPE process_result AS (
    name text,
    type text
);

CREATE OR REPLACE FUNCTION generate_resolwe_data_search(data flow_data)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        owners users_result;
        contributor users_result;
        process process_result;
        flat_descriptor text;
        search tsvector;
    BEGIN
        SELECT
            array_to_string(array_agg(username), ' ') AS usernames,
            array_to_string(array_remove(array_agg(first_name), ''), ' ') AS first_names,
            array_to_string(array_remove(array_agg(last_name), ''), ' ') AS last_names
        INTO owners
        FROM auth_user
        JOIN guardian_userobjectpermission ON auth_user.id=guardian_userobjectpermission.user_id
        WHERE
            content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='data')
            AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_data')
            AND object_pk::int=data.id
        GROUP BY object_pk;

        SELECT
            username usernames, first_name first_names, last_name last_names
        INTO contributor
        FROM auth_user
        WHERE id = data.contributor_id;

        SELECT name, type INTO process FROM flow_process WHERE id=data.process_id;

        SELECT COALESCE(flatten_descriptor_values(data.descriptor), '') INTO flat_descriptor;

        SELECT
            -- Data name.
            setweight(to_tsvector('simple', data.name), 'A') ||
            setweight(to_tsvector('simple', get_characters(data.name)), 'B') ||
            setweight(to_tsvector('simple', get_numbers(data.name)), 'B') ||
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
            -- Process name.
            setweight(to_tsvector('simple', process.name), 'B') ||
            setweight(to_tsvector('simple', get_characters(process.name)), 'C') ||
            setweight(to_tsvector('simple', get_numbers(process.name)), 'C') ||
            -- Process type.
            setweight(to_tsvector('simple', process.type), 'D') ||
            -- Data tags.
            setweight(to_tsvector('simple', array_to_string(data.tags, ' ')), 'B') ||
            -- Data descriptor.
            setweight(to_tsvector('simple', flat_descriptor), 'C') ||
            setweight(to_tsvector('simple', get_characters(flat_descriptor)), 'D') ||
            setweight(to_tsvector('simple', get_numbers(flat_descriptor)), 'D')
        INTO search;

        RETURN search;
    END;
    $$;

CREATE OR REPLACE FUNCTION data_biut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        SELECT generate_resolwe_data_search(NEW) INTO NEW.search;

        IF NEW.search IS NULL THEN
            RAISE WARNING 'Search index for data (id: %) is NULL.', NEW.id;
        END IF;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER data_biut
    BEFORE INSERT OR UPDATE
    ON flow_data
    FOR EACH ROW EXECUTE PROCEDURE data_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_data(perm guardian_userobjectpermission)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        data_content_type_id int;
        owner_data_permission_id int;
    BEGIN
        SELECT id FROM django_content_type WHERE app_label='flow' and model='data' INTO data_content_type_id;
        SELECT id FROM auth_permission WHERE codename='owner_data' INTO owner_data_permission_id;

        IF perm.content_type_id=data_content_type_id AND perm.permission_id=owner_data_permission_id THEN
            -- Set the search field to NULL to trigger data_biut.
            UPDATE flow_data SET search=NULL WHERE id=perm.object_pk::int;
        END IF;
    END;
    $$;

CREATE OR REPLACE FUNCTION userpermission_data_aiut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_data(NEW);
        RETURN NEW;
    END;
    $$;

CREATE TRIGGER userpermission_data_aiut
    AFTER INSERT OR UPDATE
    ON guardian_userobjectpermission
    FOR EACH ROW EXECUTE PROCEDURE userpermission_data_aiut();

CREATE OR REPLACE FUNCTION userpermission_data_adt()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        perform handle_userpermission_data(OLD);
        RETURN OLD;
    END;
    $$;

CREATE TRIGGER userpermission_data_adt
    AFTER DELETE
    ON guardian_userobjectpermission
    FOR EACH ROW EXECUTE PROCEDURE userpermission_data_adt();


-- Trigger after update contributor.
CREATE OR REPLACE FUNCTION data_contributor_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Set the search field to NULL to trigger data_biut.
        UPDATE flow_data SET search=NULL WHERE flow_data.contributor_id=NEW.id;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER data_contributor_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE data_contributor_aut();


-- Trigger after update owner.
CREATE OR REPLACE FUNCTION data_owner_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        WITH owner_permission AS (
            SELECT object_pk::int data_id
            FROM guardian_userobjectpermission
            WHERE
                user_id=NEW.id
                AND content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='data')
                AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_data')
        )
        -- Set the search field to NULL to trigger data_biut.
        UPDATE flow_data data
        SET search=NULL
        FROM owner_permission perm
        WHERE data.id=perm.data_id;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER data_owner_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE data_owner_aut();
