-- Trigger after insert/update Collection object.
CREATE OR REPLACE FUNCTION generate_resolwe_collection_search(collection_line flow_collection)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    DECLARE
        search tsvector;
    BEGIN
        WITH owners AS (
            SELECT
                object_pk::int collection_id,
                array_to_string(array_agg(username), ' ') AS usernames,
                array_to_string(array_remove(array_agg(first_name), ''), ' ') AS first_names,
                array_to_string(array_remove(array_agg(last_name), ''), ' ') AS last_names
            FROM auth_user
            JOIN guardian_userobjectpermission ON auth_user.id=guardian_userobjectpermission.user_id
            WHERE
                content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='collection')
                AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_collection')
                AND object_pk::int=collection_line.id
            GROUP BY object_pk
        )
        SELECT
            -- Collection name.
            setweight(to_tsvector('simple', collection.name), 'A') ||
            setweight(edge_ngrams(collection.name), 'B') ||
            setweight(edge_ngrams(get_characters(collection.name)), 'B') ||
            setweight(edge_ngrams(get_numbers(collection.name)), 'B') ||
            -- Collection description.
            setweight(to_tsvector('simple', collection.description), 'B') ||
            setweight(edge_ngrams(collection.description), 'C') ||
            setweight(edge_ngrams(get_characters(collection.description)), 'C') ||
            setweight(edge_ngrams(get_numbers(collection.description)), 'C') ||
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
            -- Collection tags.
            setweight(to_tsvector('simple', array_to_string(collection.tags, ' ')), 'B')
        FROM flow_collection collection
        JOIN owners ON collection.id=owners.collection_id
        JOIN auth_user contributor ON collection.contributor_id=contributor.id
        WHERE collection.id=collection_line.id
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

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER collection_biut
    BEFORE INSERT OR UPDATE
    ON flow_collection
    FOR EACH ROW EXECUTE PROCEDURE collection_biut();


-- Trigger after update/insert/delete user permission object.
CREATE OR REPLACE FUNCTION handle_userpermission_collection(perm guardian_userobjectpermission)
    RETURNS void
    LANGUAGE plpgsql
    AS $$
    DECLARE
        collection_content_type_id int;
        owner_collection_permission_id int;
    BEGIN
        SELECT id FROM django_content_type WHERE app_label='flow' and model='collection' INTO collection_content_type_id;
        SELECT id FROM auth_permission WHERE codename='owner_collection' INTO owner_collection_permission_id;

        IF perm.content_type_id=collection_content_type_id AND perm.permission_id=owner_collection_permission_id THEN
            -- Set the search field to NULL to trigger collection_biut.
            UPDATE flow_collection SET search=NULL WHERE id=perm.object_pk::int;
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
    ON guardian_userobjectpermission
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
    ON guardian_userobjectpermission
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
CREATE OR REPLACE FUNCTION collection_owner_aut()
    RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
    BEGIN
        WITH owner_permission AS (
            SELECT object_pk::int collection_id
            FROM guardian_userobjectpermission
            WHERE
                user_id=NEW.id
                AND content_type_id=(SELECT id FROM django_content_type WHERE app_label='flow' and model='collection')
                AND permission_id=(SELECT id FROM auth_permission WHERE codename='owner_collection')
        )
        -- Set the search field to NULL to trigger collection_biut.
        UPDATE flow_collection collection
        SET search=NULL
        FROM owner_permission perm
        WHERE collection.id=perm.collection_id;

        RETURN NEW;
    END;
    $$;

CREATE TRIGGER collection_owner_aut
    AFTER UPDATE
    ON auth_user
    FOR EACH ROW EXECUTE PROCEDURE collection_owner_aut();
