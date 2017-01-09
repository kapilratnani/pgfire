CREATE OR REPLACE FUNCTION public.patch_json_data_notify(
    jsondb_table_name regclass,
    base_key TEXT,
    insert_data jsonb, -- insert this if base_key doesn't exists
    patch_path TEXT[], -- path to patch if base_key exists
    patch_data jsonb
) RETURNS void AS
$$
BEGIN
    EXECUTE format(
    'INSERT INTO %%s (l1_key, data, created, last_modified) VALUES ($1, $2, now(), now())' ||
    ' ON CONFLICT (l1_key)' ||
    ' DO UPDATE SET data = jsonb_set_deep(%%s.data, $3, %%s.data#> $3 || $4), last_modified=now()',
        jsondb_table_name, jsondb_table_name, jsondb_table_name)
    using base_key, insert_data, patch_path, patch_data;

    PERFORM pg_notify(
        jsondb_table_name::text,
        json_build_object('event','patch', 'path', patch_path, 'data', patch_data)::text
    );
END
$$ LANGUAGE plpgsql VOLATILE STRICT;
