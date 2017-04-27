-- https://gist.github.com/tuhoojabotti/5b1fbbf3f22853475bba14f8833faada
CREATE OR REPLACE FUNCTION jsonb_set_deep(target jsonb, path text[], val jsonb)
  RETURNS jsonb AS $$
    DECLARE
      k text;
      p text[];
    BEGIN
      target = coalesce(target, '{}'::jsonb);
      -- Create missing objects in the path.
      FOREACH k IN ARRAY path LOOP
        p := p || k;
        IF (target #> p IS NULL) THEN
          target := jsonb_set(target, p, '{}'::jsonb);
        ELSIF (jsonb_typeof(target #> p) != 'object') THEN
          -- remove the value at the target and convert it to an empty object
          target := jsonb_set(target, p, '{}'::jsonb);
        END IF;
      END LOOP;
      -- Set the value like normal.
      RETURN jsonb_set(target, path, val);
    END;
  $$ LANGUAGE plpgsql;