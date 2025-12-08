GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_WURL() TO ROLE web_app;

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.INITIAL_SANITIZATION_STAGING_WURL()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Update all columns at once
    var updateQuery = `
        UPDATE upload_db.public.wurl_viewership 
        SET   
            duration = REGEXP_REPLACE(duration, ',', ''),
            tot_hov = REGEXP_REPLACE(tot_hov, ',', ''),
            tot_sessions = REGEXP_REPLACE(tot_sessions, ',', ''),
            mov_per_session = REGEXP_REPLACE(mov_per_session, ',', ''),
            unique_viewers = REGEXP_REPLACE(unique_viewers, ',', ''),
            tot_completions = REGEXP_REPLACE(tot_completions, ',', ''),
            completions_95 = REGEXP_REPLACE(completions_95, ',', ''),
            cc_inventory = REGEXP_REPLACE(cc_inventory, ',', ''),
            cc_render_rate = REGEXP_REPLACE(cc_render_rate, '[,%]', ''),
            completion_percentage = REGEXP_REPLACE(completion_percentage, '[,%]', ''),
            impressions = REGEXP_REPLACE(impressions, ',', ''),
            channel_adpool_impressions = REGEXP_REPLACE(channel_adpool_impressions, ',', ''),
            channel_adpool_revenue = REGEXP_REPLACE(channel_adpool_revenue, '[,$]', ''),
            live_event_hours = REGEXP_REPLACE(live_event_hours, ',', '');
    `;
    snowflake.execute({sqlText: updateQuery});

    return "Initial sanitization completed successfully.";
} catch (err) {
    // Error handling
    return "Error during initial sanitization: " + err.message;
}
$$;

