-- Trigger after insert/update Data object.
CREATE OR REPLACE FUNCTION generate_resolwe_data_search(data_line flow_data)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        search tsvector;
    BEGIN
        WITH owners AS (
            SELECT
                object_pk::int data_id,
                array_to_string(array_agg(username), ' ') AS usernames,
                array_to_string(array_remove(array_agg(first_name), ''), ' ') AS first_names,
                array_to_string(array_remove(array_agg(last_name), ''), ' ') AS last_names
            FROM auth_user
            JOIN guardian_userobjectpermission ON auth_user.id=guardian_userobjectpermission.user_id
            WHERE
                content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='data')
                AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_data')
                AND object_pk::int=data_line.id
            GROUP BY object_pk
        )
        SELECT
            -- Data name.
            setweight(to_tsvector('simple', data.name), 'A') ||
            setweight(edge_ngrams(data.name), 'B') ||
            setweight(edge_ngrams(get_characters(data.name)), 'B') ||
            setweight(edge_ngrams(get_numbers(data.name)), 'B') ||
            -- Contributor username.
            setweight(to_tsvector('simple', contributor.username), 'B') ||
            setweight(edge_ngrams(contributor.username), 'C') ||
            setweight(edge_ngrams(get_characters(contributor.username)), 'C') ||
            setweight(edge_ngrams(get_numbers(contributor.username)), 'C') ||
            -- Contributor first name.
            setweight(to_tsvector('simple', contributor.first_name), 'B') ||
            setweight(edge_ngrams(contributor.first_name), 'C') ||
            -- Contributor last name.
            setweight(to_tsvector('simple', contributor.last_name), 'B') ||
            setweight(edge_ngrams(contributor.last_name), 'C') ||
            -- Owners usernames.
            setweight(to_tsvector('simple', owners.usernames), 'A') ||
            setweight(edge_ngrams(owners.usernames), 'B') ||
            setweight(edge_ngrams(get_characters(owners.usernames)), 'B') ||
            setweight(edge_ngrams(get_numbers(owners.usernames)), 'B') ||
            -- Owners first names.
            setweight(to_tsvector('simple', owners.first_names), 'A') ||
            setweight(edge_ngrams(owners.first_names), 'B') ||
            -- Owners last names.
            setweight(to_tsvector('simple', owners.last_names), 'A') ||
            setweight(edge_ngrams(owners.last_names), 'B') ||
            -- Process name.
            setweight(to_tsvector('simple', process.name), 'B') ||
            setweight(edge_ngrams(process.name), 'C') ||
            setweight(edge_ngrams(get_characters(process.name)), 'C') ||
            setweight(edge_ngrams(get_numbers(process.name)), 'C') ||
            -- Process type.
            setweight(to_tsvector('simple', process.type), 'D') ||
            -- Data tags.
            setweight(to_tsvector('simple', array_to_string(data.tags, ' ')), 'B')
        FROM flow_data data
        JOIN owners ON data.id=owners.data_id
        JOIN flow_process process ON data.process_id=process.id
        JOIN auth_user contributor ON data.contributor_id=contributor.id
        WHERE data.id=data_line.id
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
