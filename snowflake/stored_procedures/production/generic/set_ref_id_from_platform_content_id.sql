-- Set ref_id from platform_content_id by matching against known ref_ids in metadata
-- OPTIMIZED VERSION: Uses single JOIN with CONTAINS instead of looping through ref_ids in JavaScript
-- CONTAINS is required because ref_id may be nested within platform_content_id string

CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_REF_ID_FROM_PLATFORM_CONTENT_ID(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    rows_updated INTEGER;
BEGIN
    -- Single UPDATE with JOIN - let Snowflake's optimizer handle the matching
    -- This is MUCH faster than looping through 50+ batches of ref_ids in JavaScript
    UPDATE NOSEY_PROD.public.platform_viewership v
    SET ref_id = e.ref_id
    FROM (
        SELECT DISTINCT ref_id
        FROM METADATA_MASTER.public.episode
        WHERE ref_id IS NOT NULL
          AND TRIM(ref_id) != ''
    ) e
    WHERE UPPER(v.platform) = UPPER(:platform)
      AND LOWER(v.filename) = LOWER(:filename)
      AND v.platform_content_id IS NOT NULL
      AND TRIM(v.platform_content_id) != ''
      AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
      AND CONTAINS(v.platform_content_id, e.ref_id);  -- CONTAINS handles nested ref_ids

    rows_updated := SQLROWCOUNT;

    RETURN 'Successfully set ref_id for ' || rows_updated || ' records from platform_content_id';
END;
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.SET_REF_ID_FROM_PLATFORM_CONTENT_ID(STRING, STRING) TO ROLE web_app;
