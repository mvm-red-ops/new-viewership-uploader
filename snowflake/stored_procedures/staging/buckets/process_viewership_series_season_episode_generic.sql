CREATE OR REPLACE PROCEDURE "PROCESS_VIEWERSHIP_SERIES_SEASON_EPISODE_GENERIC"("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    // Convert input arguments to local variables (since Snowflake arguments are uppercase)
    var platformArg = PLATFORM;
    const bucketName = "SERIES_SEASON_EPISODE"; // Hardcoded as it matches the procedure name
    const filenameArg = FILENAME;
    // Start time for procedure execution
    const startTime = new Date();
    // Function to log execution steps
    function logStep(message, status, rowsAffected = "0", errorMessage = "") {
        const executionTime = (new Date() - startTime) / 1000; // Time in seconds
        const logSql = `
        INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (
            LOG_TIME,
            LOG_MESSAGE,
            PROCEDURE_NAME,
            PLATFORM,
            STATUS,
            ROWS_AFFECTED,
            ERROR_MESSAGE,
            EXECUTION_TIME
        ) VALUES (
            CURRENT_TIMESTAMP(),
            ''${message.replace(/''/g, "''''")}'',
            ''process_viewership_series_season_episode_generic'',
            ''${platformArg.replace(/''/g, "''''")}'',
            ''${status.replace(/''/g, "''''")}'',
            ''${rowsAffected.replace(/''/g, "''''")}'',
            ''${errorMessage.replace(/''/g, "''''")}'',
            ''${executionTime}''
        )`;
        try {
            snowflake.execute({sqlText: logSql});
        } catch (logErr) {
            // If logging fails, continue with procedure but note the error
        }
    }
    // Log procedure start
    logStep(`Processing ${bucketName} bucket for platform: ${platformArg}`, "STARTED");
    // First pass: Series+Episode+Season matching (the main logic for this bucket)
    const series_episode_sql = `
    UPDATE TEST_STAGING.public.platform_viewership w
    SET
        w.series_code = q.series_code,
        w.content_provider = q.content_provider,
        w.asset_title = q.metadata_title,
        w.asset_series = q.asset_series,
        w.ref_id = q.ref_id
    FROM (
        SELECT
            v.id AS id,
            m.title AS metadata_title,
            s.content_provider AS content_provider,
            s.series_code AS series_code,
            m.ref_id as ref_id,
            UPLOAD_DB.public.extract_primary_title(s.titles) as asset_series
        FROM
            TEST_STAGING.public.platform_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON (v.id = b.id)
        JOIN METADATA_MASTER_CLEANED_STAGING.public.series s ON (UPLOAD_DB.public.extract_primary_title(s.titles) = v.internal_series)
        JOIN METADATA_MASTER_CLEANED_STAGING.public.episode e ON (s.id = e.series_id )
        JOIN METADATA_MASTER_CLEANED_STAGING.public.metadata m ON (m.ref_id = e.ref_id)
        WHERE v.platform = ''${platformArg}''
            AND v.processed IS NULL
            AND v.content_provider IS NULL
            AND v.platform_content_name IS NOT NULL
            AND v.internal_series IS NOT NULL
            AND v.episode_number IS NOT NULL
            AND v.season_number IS NOT NULL
            AND lower(s.status) = ''active''
            AND CAST(e.episode AS VARCHAR) = v.episode_number
            AND CAST(e.season AS VARCHAR) = v.season_number
            AND (m.title IS NOT NULL AND LENGTH(TRIM(m.title)) > 0)
            ${filenameArg ? `AND v.filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}
    ) q
    WHERE w.id = q.id
    `;
    logStep("Executing series+episode+season matching", "IN_PROGRESS");
    try {
        // First create a temporary table to mark records before update
        const createMarkerSql = `
        CREATE OR REPLACE TEMPORARY TABLE UPLOAD_DB.PUBLIC.${platformArg.toUpperCase()}_UPDATE_MARKER AS
        SELECT
            id,
            content_provider IS NOT NULL AS had_content_provider,
            series_code IS NOT NULL AS had_series_code,
            asset_title IS NOT NULL AS had_asset_title,
            asset_series IS NOT NULL AS had_asset_series
        FROM TEST_STAGING.public.platform_viewership
        WHERE platform = ''${platformArg}''
          AND id IN (
            SELECT id FROM UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET
        )
        `;
        snowflake.execute({sqlText: createMarkerSql});
        // Execute the series+episode+season matching
        const seriesEpisodeStmt = snowflake.createStatement({ sqlText: series_episode_sql });
        seriesEpisodeStmt.execute();
        let rows_affected = seriesEpisodeStmt.getNumRowsAffected();
        logStep(`Series+episode+season matching completed with ${rows_affected} affected rows`, "SUCCESS", rows_affected.toString());
        // Second pass: Fallback to series-only matching for remaining unmatched records
        logStep("Starting fallback: series-only matching for remaining records", "IN_PROGRESS");
        const series_only_fallback_sql = `
            UPDATE TEST_STAGING.public.platform_viewership w
            SET
                w.series_code = q.series_code,
                w.content_provider = q.content_provider,
                w.asset_title = q.asset_title,
                w.asset_series = q.asset_series,
                w.ref_id = q.ref_id
            FROM (
                SELECT
                    v.id AS id,
                    s.content_provider AS content_provider,
                    s.series_code AS series_code,
                    UPLOAD_DB.public.extract_primary_title(s.titles) as asset_series,
                    m.title as asset_title,
                    m.ref_id as ref_id
                FROM
                    TEST_STAGING.public.platform_viewership v
                JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON (v.id = b.id)
                JOIN METADATA_MASTER_CLEANED_STAGING.public.metadata m ON (
                    LOWER(REGEXP_REPLACE(TRIM(m.title),  ''[^A-Za-z0-9]'', '''')) =
                    LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name),  ''[^A-Za-z0-9]'', ''''))
                    OR
                    LOWER(REGEXP_REPLACE(TRIM(m.clean_title),  ''[^A-Za-z0-9]'', '''')) =
                    LOWER(REGEXP_REPLACE(TRIM(v.platform_content_name),  ''[^A-Za-z0-9]'', ''''))
                )
                JOIN METADATA_MASTER_CLEANED_STAGING.public.episode e ON (e.ref_id = m.ref_id)
                JOIN METADATA_MASTER_CLEANED_STAGING.public.series s ON (s.id = e.series_id)
                WHERE v.platform = ''${platformArg}''
                    AND v.processed IS NULL
                    AND v.content_provider IS NULL
                    AND v.platform_content_name IS NOT NULL
                    AND v.internal_series IS NOT NULL
                    AND lower(s.status) = ''active''
                    AND (m.title IS NOT NULL AND LENGTH(TRIM(m.title)) > 0)
                    ${filenameArg ? `AND v.filename = ''${filenameArg.replace(/''/g, "''''")}''` : ''''}
            ) q
            WHERE w.id = q.id
        `;
        try {
            const seriesOnlyStmt = snowflake.createStatement({ sqlText: series_only_fallback_sql });
            seriesOnlyStmt.execute();
            const fallback_rows_affected = seriesOnlyStmt.getNumRowsAffected();
            logStep(`Series-only fallback completed with ${fallback_rows_affected} affected rows`, "SUCCESS", fallback_rows_affected.toString());
            // Update total rows affected
            rows_affected += fallback_rows_affected;
            logStep(`Total rows affected across both passes: ${rows_affected}`, "SUCCESS", rows_affected.toString());
        } catch (fallbackErr) {
            logStep(`Warning: Series-only fallback matching failed: ${fallbackErr.toString()}`, "WARNING");
            // Continue with the procedure, don''t throw the error
        }
        // Let''s see how many records potentially match our criteria for unmatched
        const countPotentialUnmatched = `
        SELECT COUNT(*) AS POTENTIAL_UNMATCHED
        FROM TEST_STAGING.public.platform_viewership v
        JOIN UPLOAD_DB.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketName}_BUCKET b ON v.id = b.id
        WHERE v.platform = ''${platformArg}''
          AND v.content_provider IS NULL
          AND v.processed IS NULL;
        `;
        const potentialResult = snowflake.execute({sqlText: countPotentialUnmatched});
        let potentialCount = 0;
        if (potentialResult.next()) {
            potentialCount = potentialResult.getColumnValue(''POTENTIAL_UNMATCHED'');
        }
        logStep(`Found ${potentialCount} potential unmatched records in viewership table`, "INFO");
        // NOTE: SERIES_SEASON_EPISODE does NOT call handle_viewership_conflicts
        // In the original architecture, unmatched records from this bucket are handled differently
        // They have complete metadata (series+episode+season) so they go directly to flagged_metadata
        // via inline logic in the original, not through the conflicts handler
        // IMPORTANT: Log the exact return string format that will be used
        const returnStr = `Update completed successfully. Strategy: ${bucketName}, updated ${rows_affected} rows`;
        logStep(`Returning: ${returnStr}`, "INFO");
        // Log final execution stats
        const totalExecutionTime = (new Date() - startTime) / 1000;
        logStep(`Procedure completed in ${totalExecutionTime} seconds`, "COMPLETED", rows_affected.toString());
        // IMPORTANT: Make sure the return string matches the pattern expected by the main procedure
        return returnStr;
    } catch (updateErr) {
        logStep("Update operation failed", "ERROR", "0", updateErr.toString());
        throw updateErr;
    }
}
catch (err) {
    // Log the overall procedure failure
    try {
        const executionTime = (new Date() - startTime) / 1000;
        // Get the argument values, being careful in case they''re not defined
        const platformValue = typeof PLATFORM !== ''undefined'' ? PLATFORM.replace(/''/g, "''''") : ''unknown'';
        const logSql = `
        INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (
            LOG_TIME,
            LOG_MESSAGE,
            PROCEDURE_NAME,
            PLATFORM,
            STATUS,
            ROWS_AFFECTED,
            ERROR_MESSAGE,
            EXECUTION_TIME
        ) VALUES (
            CURRENT_TIMESTAMP(),
            ''Procedure execution failed'',
            ''process_viewership_series_season_episode_generic'',
            ''${platformValue}'',
            ''FAILED'',
            ''0'',
            ''${err.toString().replace(/''/g, "''''")}'',
            ''${executionTime}''
        )`;
        snowflake.execute({sqlText: logSql});
    } catch (logErr) {
        // If even error logging fails, there''s not much we can do
    }
    return "Failed: " + err;
}
';