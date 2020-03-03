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
