-- Trigger after insert/update Entity object.
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
