GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING_YOUTUBE(string) TO ROLE web_app;



CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING_YOUTUBE(filename STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
    try {
        const filename = FILENAME.toLowerCase();
        const uploadDBToStagingMutatino = `insert into test_staging.public.youtube_viewership(phase, processed, id, content_id, platform_content_name, publish_date, views, tot_hov, subscribers_gained, revenue, impressions, click_through_rate, channel, channel_id, month, year, quarter, filename, platform, partner, domain, type, territory, territory_id, deal_parent, upload_timestamp)
            SELECT phase, processed, id, content_id, platform_content_name, publish_date, views, tot_hov, subscribers_gained, estimated_revenue, impressions, click_through_rate, channel, channel_id, month, year, quarter, filename, platform, partner, domain, type, territory, territory_id, deal_parent, upload_timestamp
            from upload_db.public.youtube_viewership 
            where processed is null and lower(filename) = '${filename}';
        `;

        const mutationArray = [
            `call upload_db.public.initial_sanitization_staging_youtube()`,
            uploadDBToStagingMutatino,
            `call upload_db.public.set_phase_dynamic('Youtube', 0)`
        ];

        let step = 0;
        for (let i = 0; i < mutationArray.length; i++) {
            step = i;
            const mutation = mutationArray[i];
            snowflake.execute({sqlText: mutation});
        }

        // If all procedures are executed successfully
        console.log("All procedures executed successfully.");
        return "All procedures executed successfully.";
    } catch (error) {
        const errorMessage = "Error: " + error;
        // Log the error message to an error log table
        snowflake.execute({sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)", binds: [errorMessage, 'move_viewership_to_staging_pluto', 'Pluto']});
        return error;
    
    }
$$;
