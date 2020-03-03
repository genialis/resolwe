CREATE TYPE users_result AS (
    usernames text,
    first_names text,
    last_names text
);

CREATE OR REPLACE FUNCTION edge_ngrams(text text)
    RETURNS tsvector
    LANGUAGE sql
    AS $$
        SELECT COALESCE(
            array_to_tsvector((
                SELECT array_agg(DISTINCT substring(lexeme for len))
                FROM unnest(to_tsvector('simple', text)), generate_series(1,length(lexeme)) len
            )),
            ''::tsvector
        )
    $$;

CREATE OR REPLACE FUNCTION get_characters(text text)
    RETURNS text
    LANGUAGE sql
    AS $$
        SELECT array_to_string(
            (SELECT ARRAY (SELECT unnest(regexp_matches(text, '([a-zA-Z]+)', 'g')))),
            ' '
        )
    $$;

CREATE OR REPLACE FUNCTION get_numbers(text text)
    RETURNS text
    LANGUAGE sql
    AS $$
        SELECT array_to_string(
            (SELECT ARRAY (SELECT unnest(regexp_matches(text, '([0-9]+)', 'g')))),
            ' '
        )
    $$;

CREATE OR REPLACE FUNCTION flatten_descriptor_values(value jsonb)
    RETURNS text
    LANGUAGE sql
    AS $$
        WITH RECURSIVE tree(typeof, value) AS (
            SELECT  jsonb_typeof(value), value
            UNION ALL
            (
                -- Recursive reference to query can appear only once, so
                -- we have to make an alias.
                WITH tree AS (
                    SELECT * FROM tree
                )
                SELECT jsonb_typeof(item.value), item.value
                FROM tree
                CROSS JOIN LATERAL jsonb_each(value) item
                WHERE typeof = 'object'

                UNION ALL

                SELECT jsonb_typeof(element), element
                FROM tree
                CROSS JOIN LATERAL jsonb_array_elements(value) element
                WHERE typeof = 'array'
            )
        )
        -- #>> is used to decompose json and remove quotes from strings.
        SELECT string_agg(nullif((value #>> '{}')::text, ''), ' ')
        FROM tree
        WHERE typeof = 'string' OR typeof = 'number'
        GROUP BY true
    $$;
