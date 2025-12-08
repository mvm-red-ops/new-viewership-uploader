
CREATE OR REPLACE PROCEDURE upload_db.public.set_territory_wurl()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_command = 
     `UPDATE test_staging.public.wurl_viewership w
SET w.territory = q.tname, w.territory_id = q.tid 
FROM (
    SELECT 
        w.id AS id, 
        w.platform_partner_name,
        w.partner,
        CASE
            WHEN w.platform_partner_name IN ('Samsung-mobile', 'Samsung-web', 'Samsung', 'Samsung-hub') THEN 'us'
            ELSE split_part(w.platform_partner_name, '-', 2)
        END AS view_terr_abb,
        t.id AS tid, 
        t.name AS tname
    FROM test_staging.public.wurl_viewership w 
    LEFT JOIN dictionary.public.territories t  
        ON ARRAY_CONTAINS(upper(view_terr_abb)::variant, PARSE_JSON(UPPER(t.abbreviations)))
    WHERE w.territory IS NULL 
      AND w.processed IS NULL
      AND (w.platform_partner_name LIKE '%-%' OR w.platform_partner_name IN ('Samsung-mobile', 'Samsung-web', 'Samsung', 'Samsung-hub'))
) q
WHERE w.id = q.id;`;
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$;
