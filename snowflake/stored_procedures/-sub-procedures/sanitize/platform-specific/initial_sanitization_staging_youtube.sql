GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_YOUTUBE() TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_YOUTUBE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Update all columns at once
    var updateQuery = `
        UPDATE upload_db.public.youtube_viewership
        SET channel = 'Youtube', channel_id = 15, partner = 'Youtube',
            territory = 'United States', territory_id = 1,
            deal_parent = 42
        ;
    `;
    snowflake.execute({sqlText: updateQuery});

    return "Initial sanitization completed successfully.";
} catch (err) {
    // Error handling
    return "Error during initial sanitization: " + err.message;
}
$$;
