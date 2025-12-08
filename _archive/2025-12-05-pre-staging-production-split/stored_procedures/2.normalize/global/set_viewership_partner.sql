GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_VIEWERSHIP_PARTNER(VARCHAR) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_VIEWERSHIP_PARTNER("PLATFORM" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var sql_command = `
    UPDATE test_staging.public.${PLATFORM}_viewership v
    SET partner = src.partner
    FROM (
        SELECT 
            a.id as id,
            platform_partner_name,
            d.partner
        FROM test_staging.public.${PLATFORM}_viewership a 
        JOIN dictionary.public.deals d ON (a.deal_parent = d.id)
        WHERE a.processed IS NULL
        GROUP BY ALL
    ) src 
    WHERE v.id = src.id
    `;
    
    var result = snowflake.execute({sqlText: sql_command});
    var rows_affected = result.getRowCount();

    // Log the successful execution with full query
    var log_sql = `
    INSERT INTO upload_db.public.error_log_table (
        log_time,
        log_message,
        rows_affected
    ) VALUES (
        CURRENT_TIMESTAMP(),
        'set_viewership_partner successful for ${PLATFORM}. Executed query: ${sql_command.replace(/'/g, "''")}',
        ${rows_affected}
    )`;
    
    snowflake.execute({sqlText: log_sql});
    return "Update completed. Rows affected: " + rows_affected;
} 
catch (err) {
    // Log the error with the query that failed
    var log_sql = `
    INSERT INTO upload_db.public.error_log_table (
        log_time,
        log_message,
        rows_affected
    ) VALUES (
        CURRENT_TIMESTAMP(),
        'set_viewership_partner error for ${PLATFORM}: ${err.toString().replace(/'/g, "''")}. Failed query: ${sql_command.replace(/'/g, "''")}',
        0
    )`;
    
    snowflake.execute({sqlText: log_sql});
    return "Failed: " + err;
}
$$;