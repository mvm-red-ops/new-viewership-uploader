-- Generic Sanitization Procedure
-- Handles common data cleaning for platforms without custom sanitization requirements
-- Cleans numeric fields by removing commas, currency symbols, and non-numeric characters

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.GENERIC_SANITIZATION(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const upperPlatform = PLATFORM.toUpperCase();
    const lowerFilename = FILENAME.toLowerCase();

    // Generic sanitization for common numeric and string fields
    // This handles the most common data quality issues across platforms
    var updateQuery = `
        UPDATE upload_db.public.platform_viewership
        SET
            -- Clean numeric fields (remove commas, dollar signs, percentages)
            tot_mov = CASE
                WHEN tot_mov IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(tot_mov, '[^0-9.]', '')) = '' THEN 0
                ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(tot_mov, '[^0-9.]', '')), 25, 5)
            END,
            tot_hov = CASE
                WHEN tot_hov IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(tot_hov, '[^0-9.]', '')) = '' THEN 0
                ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(tot_hov, '[^0-9.]', '')), 25, 5)
            END,
            tot_sessions = CASE
                WHEN tot_sessions IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(tot_sessions, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(tot_sessions, '[^0-9]', '')))
            END,
            sessions = CASE
                WHEN sessions IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(sessions, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(sessions, '[^0-9]', '')))
            END,
            unique_viewers = CASE
                WHEN unique_viewers IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(unique_viewers, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(unique_viewers, '[^0-9]', '')))
            END,
            views = CASE
                WHEN views IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(views, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(views, '[^0-9]', '')))
            END,
            impressions = CASE
                WHEN impressions IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(impressions, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(impressions, '[^0-9]', '')))
            END,
            revenue = CASE
                WHEN revenue IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(revenue, '[^0-9.]', '')) = '' THEN 0
                ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(revenue, '[^0-9.]', '')), 38, 8)
            END,
            channel_adpool_revenue = CASE
                WHEN channel_adpool_revenue IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(channel_adpool_revenue, '[^0-9.]', '')) = '' THEN 0
                ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(channel_adpool_revenue, '[^0-9.]', '')), 38, 8)
            END,
            channel_adpool_impressions = CASE
                WHEN channel_adpool_impressions IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(channel_adpool_impressions, '[^0-9]', '')) = '' THEN 0
                ELSE TRY_TO_NUMBER(TRIM(REGEXP_REPLACE(channel_adpool_impressions, '[^0-9]', '')))
            END,
            duration = CASE
                WHEN duration IS NULL THEN NULL
                WHEN TRIM(REGEXP_REPLACE(duration, '[^0-9.]', '')) = '' THEN 0
                ELSE TRY_TO_DECIMAL(TRIM(REGEXP_REPLACE(duration, '[^0-9.]', '')), 25, 5)
            END,

            -- Clean episode and season numbers (remove commas)
            episode_number = CASE
                WHEN episode_number IS NULL THEN NULL
                ELSE REGEXP_REPLACE(episode_number, ',', '')
            END,
            season_number = CASE
                WHEN season_number IS NULL THEN NULL
                ELSE REGEXP_REPLACE(season_number, ',', '')
            END,

            -- Trim whitespace from text fields
            platform = TRIM(platform),
            domain = TRIM(domain),
            platform_partner_name = TRIM(platform_partner_name),
            platform_channel_name = TRIM(platform_channel_name),
            platform_territory = TRIM(platform_territory)

        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL;
    `;

    snowflake.execute({sqlText: updateQuery});

    return `Generic sanitization completed successfully for ${PLATFORM} - ${FILENAME}`;

} catch (err) {
    const errorMessage = "Error during generic sanitization: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)",
        binds: [errorMessage, 'generic_sanitization', PLATFORM]
    });

    return errorMessage;
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.GENERIC_SANITIZATION(STRING, STRING) TO ROLE web_app;
