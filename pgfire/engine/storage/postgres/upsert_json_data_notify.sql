CREATE OR REPLACE FUNCTION public.upsert_json_data_notify(
    jsondb_table_name regclass,
    base_key TEXT,
    insert_data jsonb, -- insert this if base_key doesn't exists
    update_path TEXT[], -- path to update if base_key exists
    update_data jsonb
) RETURNS void AS
$$
BEGIN
    EXECUTE format(
    'INSERT INTO %%s (l1_key, data, created, last_modified) VALUES ($1, $2, now(), now())' ||
    ' ON CONFLICT (l1_key)' ||
    ' DO UPDATE SET data = jsonb_set_deep(%%s.data, $3, $4), last_modified=now()', jsondb_table_name, jsondb_table_name)
    using base_key, insert_data, update_path, update_data;

    PERFORM pg_notify(
        jsondb_table_name::text,
        json_build_object('event', 'put', 'path', update_path, 'data', update_data)::text
    );
END
$$ LANGUAGE plpgsql VOLATILE STRICT;

