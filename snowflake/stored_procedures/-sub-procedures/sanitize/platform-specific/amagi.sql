CREATE OR REPLACE PROCEDURE upload_db.public.initial_sanitization_staging_amagi()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Update all columns at once
    var updateQuery = `
    select * from test_staging.public.amagi_viewership`;
    snowflake.execute({sqlText: updateQuery});

    return "Initial sanitization completed successfully.";
} catch (err) {
    // Error handling
    return "Error during initial sanitization: " + err.message;
}
$$;