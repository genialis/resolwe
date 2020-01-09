CREATE OR REPLACE FUNCTION edge_ngrams(text text)
    RETURNS tsvector
    LANGUAGE plpgsql
    AS $$
    BEGIN
        RETURN (
            SELECT COALESCE(
                array_to_tsvector((
                    SELECT array_agg(DISTINCT substring(lexeme for len))
                    FROM unnest(to_tsvector('simple', text)), generate_series(1,length(lexeme)) len
                )),
                ''::tsvector
            )
        );
    END;
    $$;

CREATE OR REPLACE FUNCTION get_characters(text text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
    BEGIN
        RETURN array_to_string(
            (SELECT ARRAY (SELECT unnest(regexp_matches(text, '([a-zA-Z]+)', 'g')))),
            ' '
        );
    END;
    $$;

CREATE OR REPLACE FUNCTION get_numbers(text text)
    RETURNS text
    LANGUAGE plpgsql
    AS $$
    BEGIN
        RETURN array_to_string(
            (SELECT ARRAY (SELECT unnest(regexp_matches(text, '([0-9]+)', 'g')))),
            ' '
        );
    END;
    $$;
