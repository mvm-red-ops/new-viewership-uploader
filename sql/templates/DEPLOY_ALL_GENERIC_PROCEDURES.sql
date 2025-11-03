-- ==============================================================================
-- ALL GENERIC PROCEDURES - MAIN DEPLOYMENT FILE
-- ==============================================================================
-- This file contains ALL the main stored procedures for the generic platform_viewership
-- table architecture.
--
-- IMPORTANT: Deploy in this order:
--   1. Run DEPLOY_GENERIC_CONTENT_REFERENCES.sql FIRST (sub-procedures)
--   2. Then run this file SECOND (DEPLOY_ALL_GENERIC_PROCEDURES.sql)
--
-- Procedures in this file:
-- SHARED:
--   1. set_phase_generic - Updates phase for records
--   2. calculate_viewership_metrics - Calculates missing TOT_HOV/TOT_MOV
--   3. set_date_columns_dynamic - Sets all date columns (full_date, week, quarter, year, month, year_month_day, day)
--   4. handle_viewership_conflicts - Tracks unmatched records into flagged_metadata (called from each bucket)
--
-- PHASE 2:
--   5. set_deal_parent_generic - Primary: Sets all normalized fields from active_deals
--   6. set_channel_generic - Fallback: Pattern matching for channel
--   7. set_territory_generic - Fallback: Normalizes territory names
--   8. send_unmatched_deals_alert - Sends email for unmatched records
--   9. set_internal_series_generic - Matches platform series to internal dictionary
--   10. analyze_and_process_viewership_data_generic - Orchestrates bucket-based asset matching
--       (calls sub-procedures from DEPLOY_GENERIC_CONTENT_REFERENCES.sql)
--
-- PHASE 3:
--   11. move_data_to_final_table_dynamic_generic - Moves data to final table
--   12. handle_final_insert_dynamic_generic - Orchestrates Phase 3
--
-- ==============================================================================

-- ==============================================================================
-- SHARED PROCEDURE: set_phase_generic
-- ==============================================================================
-- Updates the processing phase for records in platform_viewership table
-- Used across all phases
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_phase_generic(
    platform VARCHAR,
    phase_number FLOAT,
    filename VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const upperPlatform = PLATFORM.toUpperCase();
    const jsPhaseNumber = PHASE_NUMBER;
    const lowerFilename = FILENAME.toLowerCase();

    // Update phase for all unprocessed records matching platform and filename
    var sql_command = `
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET phase = ?
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND processed IS NULL
    `;

    var statement = snowflake.createStatement({
        sqlText: sql_command,
        binds: [jsPhaseNumber]
    });

    var result = statement.execute();

    // Get the number of rows updated
    var rowCountQuery = `
        SELECT COUNT(*) as cnt
        FROM {{STAGING_DB}}.public.platform_viewership
        WHERE UPPER(platform) = '${upperPlatform}'
          AND LOWER(filename) = '${lowerFilename}'
          AND phase = ${jsPhaseNumber}
          AND processed IS NULL
    `;

    var rowCountStmt = snowflake.createStatement({sqlText: rowCountQuery});
    var rowCountResult = rowCountStmt.execute();
    rowCountResult.next();
    var rowCount = rowCountResult.getColumnValue('CNT');

    return `Phase set to ${jsPhaseNumber} for ${rowCount} records (${PLATFORM} - ${FILENAME})`;

} catch (err) {
    const errorMessage = "Error in set_phase_generic: " + err.message;

    // Log the error
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)",
        binds: [errorMessage, 'set_phase_generic', PLATFORM]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_phase_generic(VARCHAR, FLOAT, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- SHARED PROCEDURE: calculate_viewership_metrics
-- ==============================================================================
-- Calculates TOT_HOV from TOT_MOV or TOT_MOV from TOT_HOV when only one exists
-- Should only be called for "Viewership" type data (not Revenue)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.calculate_viewership_metrics(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;
    let updatedRecords = 0;

    // Calculate TOT_HOV from TOT_MOV (minutes to hours) when TOT_HOV is null
    const calculateHoursSQL = `
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET tot_hov = tot_mov / 60.0
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND tot_mov IS NOT NULL
          AND tot_hov IS NULL
          AND processed IS NULL
    `;

    console.log("Calculating TOT_HOV from TOT_MOV...");
    const hoursStmt = snowflake.createStatement({sqlText: calculateHoursSQL});
    hoursStmt.execute();
    const hoursAffected = hoursStmt.getNumRowsAffected();
    updatedRecords += hoursAffected;
    console.log(`Calculated TOT_HOV for ${hoursAffected} records`);

    // Calculate TOT_MOV from TOT_HOV (hours to minutes) when TOT_MOV is null
    const calculateMinutesSQL = `
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET tot_mov = tot_hov * 60.0
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND tot_hov IS NOT NULL
          AND tot_mov IS NULL
          AND processed IS NULL
    `;

    console.log("Calculating TOT_MOV from TOT_HOV...");
    const minutesStmt = snowflake.createStatement({sqlText: calculateMinutesSQL});
    minutesStmt.execute();
    const minutesAffected = minutesStmt.getNumRowsAffected();
    updatedRecords += minutesAffected;
    console.log(`Calculated TOT_MOV for ${minutesAffected} records`);

    // Log to error table
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Calculated missing viewership metrics. Total records updated: ${updatedRecords}`, 'calculate_viewership_metrics', PLATFORM]
    });

    return `Successfully calculated missing metrics for ${updatedRecords} records`;

} catch (error) {
    const errorMessage = "Error in calculate_viewership_metrics: " + error.message;
    console.error(errorMessage);

    // Log error
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'calculate_viewership_metrics', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.calculate_viewership_metrics(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- SHARED PROCEDURE: set_date_columns_dynamic
-- ==============================================================================
-- Sets all date-related columns from the DATE column
-- Columns set: full_date, week, quarter, year, month, year_month_day, day
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_date_columns_dynamic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Function to execute a given SQL command and return a status message
    function executeSqlCommand(sql_command) {
        try {
            snowflake.execute({sqlText: sql_command});
            return "Succeeded";
        } catch (err) {
            return "Failed: " + err;
        }
    }

    const platform = PLATFORM;
    const filename = FILENAME;

    var updateCommands = [
        // Update statement for getting the full date
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET full_date = {{UPLOAD_DB}}.public.get_full_date(date)
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the week start date
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET week = {{UPLOAD_DB}}.public.get_week_start({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the quarter
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET quarter = {{UPLOAD_DB}}.public.get_quarter_from_mm_dd_yyyy({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the year
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET year = {{UPLOAD_DB}}.public.get_year_from_mm_dd_yyyy({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the month
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET month = {{UPLOAD_DB}}.public.get_month_from_mm_dd_yyyy({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the first of the month
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET year_month_day = {{UPLOAD_DB}}.public.get_first_of_month_from_mm_dd_yyyy({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`,

        // Update statement for setting the day
        `UPDATE {{STAGING_DB}}.public.platform_viewership
         SET day = {{UPLOAD_DB}}.public.get_day_from_mm_dd_yyyy({{UPLOAD_DB}}.public.get_full_date(date))
         WHERE platform = '${platform}'
           AND filename = '${filename}'
           AND processed IS NULL`
    ];

    var resultMessage = "";
    for (var i = 0; i < updateCommands.length; i++) {
        resultMessage = executeSqlCommand(updateCommands[i]);
        if (resultMessage !== "Succeeded") {
            return "Error executing update command: " + resultMessage;
        }
    }

    return "All date columns set successfully for " + platform + " - " + filename;
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_date_columns_dynamic(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- SHARED PROCEDURE: handle_viewership_conflicts
-- ==============================================================================
-- Tracks unmatched records from asset matching buckets into appropriate conflict tables
-- Called at the end of each bucket matching path to capture conflicts
--
-- Routing Logic:
--   - FULL_DATA bucket → {{METADATA_DB}}.public.flagged_metadata
--     (complete metadata: has ref_id + internal_series + episode + season)
--   - All other buckets → {{METADATA_DB}}.public.conflicts
--     (incomplete metadata: missing ref_id or other required fields)
--
-- Parameters:
--   - platform: Platform being processed
--   - filename: Source filename
--   - conflict_type: Description of why records didn't match
--   - bucket_name: Name of the bucket/matching strategy (FULL_DATA, SERIES_SEASON_EPISODE, etc.)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.handle_viewership_conflicts(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR,
    "CONFLICT_TYPE" VARCHAR,
    "BUCKET_NAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;
    const conflictType = CONFLICT_TYPE;
    const bucketName = BUCKET_NAME;
    const platformUpper = platform.toUpperCase();

    // Build notes string with all context
    const notesPrefix = `Platform: ${platform}, Bucket: ${bucketName}, Conflict: ${conflictType}, Filename: ${filename}, Date: `;

    // Route to different tables based on bucket type:
    // - FULL_DATA only → flagged_metadata (complete metadata: has ref_id + internal_series + episode + season)
    // - All others including SERIES_SEASON_EPISODE → conflicts (incomplete metadata: missing ref_id or other fields)
    const useFlaggedMetadata = (bucketName === 'FULL_DATA');

    let conflictDataQuery = '';

    if (useFlaggedMetadata) {
        // For FULL_DATA only: Use flagged_metadata
        // These records have complete metadata (ref_id, internal_series, season, episode) but still failed to match
        conflictDataQuery = `
            MERGE INTO {{METADATA_DB}}.public.flagged_metadata T
            USING (
                SELECT
                    v.platform_content_name AS title,
                    v.ref_id,
                    v.internal_series,
                    v.season_number,
                    v.episode_number,
                    '${notesPrefix}' || COALESCE(TO_VARCHAR(MIN(v.month)), 'Unknown') || '/' || COALESCE(TO_VARCHAR(MIN(v.year)), 'Unknown') as notes
                FROM {{STAGING_DB}}.public.platform_viewership v
                JOIN {{UPLOAD_DB}}.PUBLIC.TEMP_${platformUpper}_${bucketName}_BUCKET b ON v.id = b.id
                WHERE v.platform = '${platform}'
                  AND v.filename = '${filename}'
                  AND v.content_provider IS NULL
                  AND v.processed IS NULL
                GROUP BY v.platform_content_name, v.ref_id, v.internal_series, v.season_number, v.episode_number
            ) S
            ON (T.title = S.title OR (T.title IS NULL AND S.title IS NULL))
            AND (T.ref_id = S.ref_id OR (T.ref_id IS NULL AND S.ref_id IS NULL))
            AND (T.internal_series = S.internal_series OR (T.internal_series IS NULL AND S.internal_series IS NULL))
            AND (T.season_number = S.season_number OR (T.season_number IS NULL AND S.season_number IS NULL))
            AND (T.episode_number = S.episode_number OR (T.episode_number IS NULL AND S.episode_number IS NULL))

            -- If match exists, append to notes
            WHEN MATCHED THEN UPDATE SET
                T.notes = T.notes || '; ' || S.notes

            -- If no match, insert new conflict row
            WHEN NOT MATCHED THEN INSERT (
                title,
                ref_id,
                internal_series,
                season_number,
                episode_number,
                notes
            ) VALUES (
                S.title,
                S.ref_id,
                S.internal_series,
                S.season_number,
                S.episode_number,
                S.notes
            )
        `;
    } else {
        // For SERIES_SEASON_EPISODE, REF_ID_ONLY, REF_ID_SERIES, SERIES_ONLY, TITLE_ONLY: Use conflicts
        // These records have incomplete metadata (missing ref_id or other fields) and need manual review
        conflictDataQuery = `
            MERGE INTO {{METADATA_DB}}.public.conflicts T
            USING (
                SELECT
                    MIN(v.id) AS id,
                    MAX(v.ref_id) AS ref_id,
                    v.platform_content_name AS title,
                    NULL AS episode_id,
                    '${notesPrefix}' || COALESCE(TO_VARCHAR(MIN(v.month)), 'Unknown') || '/' || COALESCE(TO_VARCHAR(MIN(v.year)), 'Unknown') as notes,
                    '${filename}' as filename,
                    NULL as processed
                FROM {{STAGING_DB}}.public.platform_viewership v
                JOIN {{UPLOAD_DB}}.PUBLIC.TEMP_${platformUpper}_${bucketName}_BUCKET b ON v.id = b.id
                WHERE v.platform = '${platform}'
                  AND v.filename = '${filename}'
                  AND v.content_provider IS NULL
                  AND v.processed IS NULL
                GROUP BY v.platform_content_name
            ) S
            ON (T.title = S.title OR (T.title IS NULL AND S.title IS NULL))

            -- If match exists, append to notes
            WHEN MATCHED THEN UPDATE SET
                T.notes = T.notes || '; ' || S.notes

            -- If no match, insert new conflict row
            WHEN NOT MATCHED THEN INSERT (
                id,
                ref_id,
                title,
                episode_id,
                notes,
                filename,
                processed
            ) VALUES (
                S.id,
                S.ref_id,
                S.title,
                S.episode_id,
                S.notes,
                S.filename,
                S.processed
            )
        `;
    }

    const insertConflictStatement = snowflake.createStatement({sqlText: conflictDataQuery});
    insertConflictStatement.execute();
    const rowsFlagged = insertConflictStatement.getNumRowsAffected();

    const targetTable = useFlaggedMetadata ? '{{METADATA_DB}}.public.flagged_metadata' : '{{METADATA_DB}}.public.conflicts';

    // Insert individual records into record_reprocessing_batch_logs
    // This gives Lambda the exact count of failed individual records by filename
    // Note: conflicts/flagged_metadata have one row per unique title (MERGED)
    //       but record_reprocessing_batch_logs has one row per viewership_id
    const logRecordsSql = `
        INSERT INTO {{METADATA_DB}}.public.record_reprocessing_batch_logs (
            title,
            viewership_id,
            filename,
            notes,
            platform
        )
        SELECT
            v.platform_content_name,
            v.id,
            v.filename,
            'Conflict Record: ${conflictType}',
            '${platform}'
        FROM {{STAGING_DB}}.public.platform_viewership v
        JOIN {{UPLOAD_DB}}.PUBLIC.TEMP_${platformUpper}_${bucketName}_BUCKET b ON v.id = b.id
        WHERE v.platform = '${platform}'
          AND v.filename = '${filename}'
          AND v.content_provider IS NULL
          AND v.processed IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM {{METADATA_DB}}.public.record_reprocessing_batch_logs l
              WHERE l.viewership_id = v.id
          )
    `;

    const insertRecordsStatement = snowflake.createStatement({sqlText: logRecordsSql});
    insertRecordsStatement.execute();
    const rowsLogged = insertRecordsStatement.getNumRowsAffected();

    // Log to error_log_table
    const logSql = `
        INSERT INTO {{UPLOAD_DB}}.public.error_log_table (
            log_time,
            log_message,
            procedure_name,
            platform,
            status,
            rows_affected
        ) VALUES (
            CURRENT_TIMESTAMP(),
            'Inserted ${rowsFlagged} unmatched records from ${bucketName} bucket into ${targetTable} and ${rowsLogged} individual records into record_reprocessing_batch_logs: ${conflictType}',
            'handle_viewership_conflicts',
            '${platform}',
            'WARNING',
            ${rowsLogged}
        )
    `;

    snowflake.execute({sqlText: logSql});

    return `Inserted ${rowsFlagged} conflict records into ${targetTable} and ${rowsLogged} individual records into record_reprocessing_batch_logs from ${bucketName} bucket (${platform} - ${filename})`;

} catch (err) {
    const errorMessage = "Error in handle_viewership_conflicts: " + err.message;

    // Log the error
    try {
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, status, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, 'ERROR', ?)",
            binds: [errorMessage, 'handle_viewership_conflicts', PLATFORM, err.message]
        });
    } catch (logErr) {
        // If even logging fails, just return the error
    }

    // Return error but don't throw - this allows the pipeline to continue even if conflict tracking fails
    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.handle_viewership_conflicts(VARCHAR, VARCHAR, VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 1: set_deal_parent_generic
-- ==============================================================================
-- Sets deal_parent by looking up from dictionary.public.active_deals table
-- Matches based on partner and content_provider
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_deal_parent_generic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    // Update deal_parent AND all normalized fields by joining with active_deals table
    // Dynamically match on platform + whichever fields are NOT NULL
    // Match on: platform (always) + platform_partner_name (if not null) + platform_channel_name (if not null) + platform_territory (if not null)
    // Sets: deal_parent, partner, channel, territory, channel_id, territory_id
    const updateSql = `
        UPDATE {{STAGING_DB}}.public.platform_viewership v
        SET
            deal_parent = ad.deal_parent,
            partner = ad.internal_partner,
            channel = ad.internal_channel,
            territory = ad.internal_territory,
            channel_id = ad.internal_channel_id,
            territory_id = ad.internal_territory_id
        FROM dictionary.public.active_deals ad
        WHERE ad.platform = '${platform}'
          AND UPPER(ad.domain) = UPPER(v.domain)
          AND ad.active = true
          AND (v.platform_partner_name IS NULL OR UPPER(v.platform_partner_name) = UPPER(ad.platform_partner_name))
          AND (v.platform_channel_name IS NULL OR UPPER(v.platform_channel_name) = UPPER(ad.platform_channel_name))
          AND (v.platform_territory IS NULL OR UPPER(v.platform_territory) = UPPER(ad.platform_territory))
          AND v.filename = '${filename}'
          AND v.platform = '${platform}'
          AND v.deal_parent IS NULL
          AND v.processed IS NULL
    `;

    console.log("Setting deal_parent from active_deals...");
    const stmt = snowflake.createStatement({sqlText: updateSql});
    stmt.execute();
    const rowsAffected = stmt.getNumRowsAffected();
    console.log(`Updated deal_parent for ${rowsAffected} records`);

    // Log to error table
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Set deal_parent for ${rowsAffected} records`, 'set_deal_parent_generic', PLATFORM]
    });

    // Check for records without deal_parent (no matching deals)
    const checkSql = `
        SELECT COUNT(*) as unmatched_count
        FROM {{STAGING_DB}}.public.platform_viewership
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND deal_parent IS NULL
          AND processed IS NULL
    `;

    const checkStmt = snowflake.createStatement({sqlText: checkSql});
    const checkResult = checkStmt.execute();
    let unmatchedCount = 0;
    if (checkResult.next()) {
        unmatchedCount = checkResult.getColumnValue(1);
    }

    if (unmatchedCount > 0) {
        console.log(`Warning: ${unmatchedCount} records still have NULL deal_parent`);

        // Log warning
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`WARNING: ${unmatchedCount} records have no matching active deals`, 'set_deal_parent_generic', PLATFORM]
        });
    }

    return `Successfully set deal_parent for ${rowsAffected} records. ${unmatchedCount} records remain without deal_parent.`;

} catch (error) {
    const errorMessage = "Error in set_deal_parent_generic: " + error.message;
    console.error(errorMessage);

    // Log error
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'set_deal_parent_generic', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_deal_parent_generic(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 2: set_channel_generic (Fallback)
-- ==============================================================================
-- Pattern matching fallback for records that didn't match active_deals
-- Uses CONTAINS to identify channel from platform_channel_name
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_channel_generic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    // Pattern-match channel from platform_channel_name for unmatched records
    const updateSql = `
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET channel = CASE
            WHEN CONTAINS(LOWER(platform_channel_name), 'confess') THEN 'Confess by Nosey'
            WHEN CONTAINS(LOWER(platform_channel_name), 'judge') OR CONTAINS(LOWER(platform_channel_name), 'real') THEN 'Judge Nosey'
            WHEN CONTAINS(LOWER(platform_channel_name), 'presented') THEN 'Presented by Nosey'
            WHEN CONTAINS(LOWER(platform_channel_name), 'escandalos') THEN 'Nosey Escandalos'
            ELSE 'Nosey'
        END,
        channel_id = CASE
            WHEN CONTAINS(LOWER(platform_channel_name), 'confess') THEN 16
            WHEN CONTAINS(LOWER(platform_channel_name), 'judge') OR CONTAINS(LOWER(platform_channel_name), 'real') THEN 10
            WHEN CONTAINS(LOWER(platform_channel_name), 'presented') THEN 17
            WHEN CONTAINS(LOWER(platform_channel_name), 'escandalos') THEN 13
            ELSE 8
        END
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND channel IS NULL
          AND processed IS NULL
    `;

    console.log("Setting channel using pattern matching...");
    const stmt = snowflake.createStatement({sqlText: updateSql});
    stmt.execute();
    const rowsAffected = stmt.getNumRowsAffected();
    console.log(`Set channel for ${rowsAffected} records using pattern matching`);

    // Log to error table
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Set channel for ${rowsAffected} records using pattern matching`, 'set_channel_generic', PLATFORM]
    });

    return `Successfully set channel for ${rowsAffected} records`;

} catch (error) {
    const errorMessage = "Error in set_channel_generic: " + error.message;
    console.error(errorMessage);

    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'set_channel_generic', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_channel_generic(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 3: set_territory_generic (Fallback)
-- ==============================================================================
-- Normalizes territory names for records that didn't match active_deals
-- Handles common territory abbreviations and variants
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_territory_generic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    // Normalize territory from platform_territory for unmatched records
    const updateSql = `
        UPDATE {{STAGING_DB}}.public.platform_viewership
        SET territory = CASE
            WHEN UPPER(platform_territory) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
            WHEN UPPER(platform_territory) IN ('UK', 'GB', 'UNITED KINGDOM', 'GREAT BRITAIN') THEN 'United Kingdom'
            WHEN UPPER(platform_territory) IN ('CA', 'CANADA') THEN 'Canada'
            WHEN UPPER(platform_territory) IN ('AU', 'AUSTRALIA') THEN 'Australia'
            WHEN UPPER(platform_territory) IN ('NZ', 'NEW ZEALAND') THEN 'New Zealand'
            WHEN UPPER(platform_territory) IN ('INTL', 'INTERNATIONAL', 'UNSPECIFIED') THEN 'International'
            ELSE platform_territory
        END,
        territory_id = CASE
            WHEN UPPER(platform_territory) IN ('US', 'USA', 'UNITED STATES') THEN 1
            WHEN UPPER(platform_territory) IN ('UK', 'GB', 'UNITED KINGDOM', 'GREAT BRITAIN') THEN 5
            WHEN UPPER(platform_territory) IN ('CA', 'CANADA') THEN 4
            WHEN UPPER(platform_territory) IN ('AU', 'AUSTRALIA') THEN 10
            WHEN UPPER(platform_territory) IN ('NZ', 'NEW ZEALAND') THEN 13
            WHEN UPPER(platform_territory) IN ('INTL', 'INTERNATIONAL', 'UNSPECIFIED') THEN 2
            ELSE 0
        END
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND territory IS NULL
          AND platform_territory IS NOT NULL
          AND processed IS NULL
    `;

    console.log("Normalizing territory...");
    const stmt = snowflake.createStatement({sqlText: updateSql});
    stmt.execute();
    const rowsAffected = stmt.getNumRowsAffected();
    console.log(`Normalized territory for ${rowsAffected} records`);

    // Log to error table
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Normalized territory for ${rowsAffected} records`, 'set_territory_generic', PLATFORM]
    });

    return `Successfully normalized territory for ${rowsAffected} records`;

} catch (error) {
    const errorMessage = "Error in set_territory_generic: " + error.message;
    console.error(errorMessage);

    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'set_territory_generic', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_territory_generic(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 4: send_unmatched_deals_alert
-- ==============================================================================
-- Sends email notification for records that still don't have deal_parent
-- after all matching attempts
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.send_unmatched_deals_alert(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platform = PLATFORM;
    const filename = FILENAME;

    // Get records without deal_parent
    const checkSql = `
        SELECT
            platform_partner_name,
            platform_channel_name,
            platform_territory,
            COUNT(*) as record_count
        FROM {{STAGING_DB}}.public.platform_viewership
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND deal_parent IS NULL
          AND processed IS NULL
        GROUP BY platform_partner_name, platform_channel_name, platform_territory
    `;

    const checkStmt = snowflake.createStatement({sqlText: checkSql});
    const checkResult = checkStmt.execute();

    let unmatchedRecords = [];
    while (checkResult.next()) {
        unmatchedRecords.push({
            partner: checkResult.getColumnValue(1) || 'NULL',
            channel: checkResult.getColumnValue(2) || 'NULL',
            territory: checkResult.getColumnValue(3) || 'NULL',
            count: checkResult.getColumnValue(4)
        });
    }

    if (unmatchedRecords.length > 0) {
        // Build email body
        let emailBody = `<html><body>`;
        emailBody += `<h2>⚠️ Unmatched Records Alert</h2>`;
        emailBody += `<p><strong>Platform:</strong> ${platform}</p>`;
        emailBody += `<p><strong>Filename:</strong> ${filename}</p>`;
        emailBody += `<p>The following records could not find a matching deal in active_deals table:</p>`;
        emailBody += `<table border="1" cellpadding="8" style="border-collapse: collapse;">`;
        emailBody += `<thead><tr style="background-color: #f2f2f2;">`;
        emailBody += `<th>Platform Partner Name</th><th>Platform Channel Name</th><th>Platform Territory</th><th>Record Count</th>`;
        emailBody += `</tr></thead><tbody>`;

        for (let i = 0; i < unmatchedRecords.length; i++) {
            const rec = unmatchedRecords[i];
            emailBody += `<tr>`;
            emailBody += `<td>${rec.partner}</td>`;
            emailBody += `<td>${rec.channel}</td>`;
            emailBody += `<td>${rec.territory}</td>`;
            emailBody += `<td>${rec.count}</td>`;
            emailBody += `</tr>`;
        }

        emailBody += `</tbody></table>`;
        emailBody += `<p><strong>Action Required:</strong> Add matching records to dictionary.public.active_deals or review the platform data.</p>`;
        emailBody += `</body></html>`;

        // Send email
        const sendEmailSql = `
            CALL SYSTEM$SEND_EMAIL(
                'SNOWFLAKE_EMAIL_SENDER',
                'tayloryoung@mvmediasales.com, data@nosey.com',
                '🚨 Unmatched Deals Alert - ${platform}',
                ?,
                'text/html'
            )
        `;

        snowflake.execute({
            sqlText: sendEmailSql,
            binds: [emailBody]
        });

        console.log(`Sent unmatched deals alert email for ${unmatchedRecords.length} unique combinations`);

        // Log to error table
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Sent unmatched deals alert for ${unmatchedRecords.length} combinations`, 'send_unmatched_deals_alert', PLATFORM]
        });

        return `Sent alert email for ${unmatchedRecords.length} unmatched record combinations`;
    } else {
        console.log('All records have deal_parent - no alert needed');
        return 'All records matched successfully';
    }

} catch (error) {
    const errorMessage = "Error in send_unmatched_deals_alert: " + error.message;
    console.error(errorMessage);

    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'send_unmatched_deals_alert', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.send_unmatched_deals_alert(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 5: set_internal_series_generic
-- ==============================================================================
-- Matches platform_series from viewership data to validated series names
-- in dictionary.public.series table
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.set_internal_series_generic("PLATFORM" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var platformArg = PLATFORM;
    var filenameArg = FILENAME;

    // Function to update internal_series based on dictionary.public.series
    function setInternalSeries() {
        try {
            // Perform the update
                var sqlText = `
                    UPDATE {{STAGING_DB}}.public.platform_viewership
                    SET internal_series = x.validated_series
                    FROM (
                        SELECT
                            v.platform_series,
                            s.series AS validated_series
                        FROM {{STAGING_DB}}.public.platform_viewership v
                        JOIN dictionary.public.series s
                        ON (LOWER(REGEXP_REPLACE(s.entry, '[^A-Za-z0-9 ]', '')) = LOWER(REGEXP_REPLACE(v.platform_series, '[^A-Za-z0-9 ]', '')))
                        WHERE v.platform = '${platformArg}'
                        AND v.processed IS NULL
                        AND v.filename = '${filenameArg}'
                        AND v.platform_series IS NOT NULL
                        GROUP BY all
                    ) x
                    WHERE {{STAGING_DB}}.public.platform_viewership.platform_series = x.platform_series
                    AND {{STAGING_DB}}.public.platform_viewership.platform = '${platformArg}'
                `;

            snowflake.execute({sqlText: sqlText});
            return "Succeeded";
        } catch (err) {
            return "Failed: " + err;
        }
    }

    // Execute the function
    return setInternalSeries();
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.set_internal_series_generic(VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 2 PROCEDURE 6: analyze_and_process_viewership_data_generic
-- ==============================================================================
-- Performs bucket-based asset matching by:
-- 1. Categorizing records into buckets based on available fields
-- 2. Calling specialized sub-procedures for each bucket
-- 3. Tracking unmatched records between bucket processing
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.analyze_and_process_viewership_data_generic(
    "PLATFORM" VARCHAR,
    "FILENAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const platformArg = PLATFORM;
const filenameArg = FILENAME;

// Use generic platform_viewership table
const viewershipTable = `{{STAGING_DB}}.public.platform_viewership`;

// Build base conditions with platform filter
const baseConditions = `platform = '${platformArg}'
AND processed IS NULL
AND content_provider IS NULL
AND platform_content_name IS NOT NULL
${filenameArg ? `AND filename = '${filenameArg.replace(/'/g, "''")}'` : ''}`;

try {
    // Start time for procedure execution
    const startTime = new Date();

    // Function to log execution steps
    function logStep(message, status, rowsAffected = "0", errorMessage = "") {
        const executionTime = (new Date() - startTime) / 1000;
        const logSql = `
        INSERT INTO {{UPLOAD_DB}}.PUBLIC.ERROR_LOG_TABLE (
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
            '${message.replace(/'/g, "''")}',
            'analyze_and_process_viewership_data_generic',
            '${platformArg.replace(/'/g, "''")}',
            '${status.replace(/'/g, "''")}',
            '${rowsAffected.replace(/'/g, "''")}',
            '${errorMessage.replace(/'/g, "''")}',
            '${executionTime}'
        )`;

        try {
            snowflake.execute({sqlText: logSql});
        } catch (logErr) {
            // If logging fails, continue with procedure but note the error
        }
    }

    // Log procedure start
    logStep(`Starting procedure for platform: ${platformArg}, filename: ${filenameArg || 'ALL'}`, "STARTED");

    // Check if there are any records in the viewership table matching our criteria
    const recordCheckSql = `
        SELECT COUNT(*) AS RECORD_COUNT
        FROM ${viewershipTable}
        WHERE ${baseConditions}`;

    try {
        const recordCheckResult = snowflake.execute({sqlText: recordCheckSql});
        let recordCount = 0;
        if (recordCheckResult.next()) {
            recordCount = recordCheckResult.getColumnValue('RECORD_COUNT');
        }

        logStep(`Found ${recordCount} records to process in ${viewershipTable}`, "INFO");

        if (recordCount === 0) {
            const msg = `No records to process for platform ${platformArg}${filenameArg ? `, filename ${filenameArg}` : ''}`;
            logStep(msg, "COMPLETED", "0");
            return msg;
        }
    } catch (err) {
        logStep(`Error checking for records: ${err.toString()}`, "ERROR");
        throw new Error(`Failed to access ${viewershipTable}: ${err.toString()}`);
    }

    // Define bucket categories based on what fields exist in viewership data
    const bucketCategories = [
        ["FULL_DATA", true, true, true, true],
        ["REF_ID_SERIES", true, true, null, null],
        ["REF_ID_ONLY", true, false, null, null],
        ["SERIES_SEASON_EPISODE", false, true, true, true],
        ["SERIES_ONLY", false, true, null, null],
        ["TITLE_ONLY", null, null, null, null]
    ];

    const bucketOrder = ["FULL_DATA", "REF_ID_SERIES", "REF_ID_ONLY", "SERIES_SEASON_EPISODE", "SERIES_ONLY", "TITLE_ONLY"];

    // Drop unmatched records table if it exists from previous runs
    try {
        snowflake.execute({
            sqlText: `DROP TABLE IF EXISTS {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`
        });
        logStep("Dropped previous unmatched records table if it existed", "INFO");
    } catch (err) {
        logStep(`Warning: Error dropping unmatched records table: ${err.toString()}`, "WARNING");
    }

    // Delete old conflict records from previous runs of this same filename
    // This ensures Lambda gets accurate unmatched count for the current batch only
    if (filenameArg) {
        try {
            const deleteResult = snowflake.execute({
                sqlText: `DELETE FROM {{METADATA_DB}}.public.record_reprocessing_batch_logs WHERE filename = '${filenameArg.replace(/'/g, "''")}'`
            });
            const deletedCount = deleteResult.getNumRowsAffected();
            logStep(`Deleted ${deletedCount} old conflict records for filename: ${filenameArg}`, "INFO");
        } catch (err) {
            logStep(`Warning: Error deleting old conflict records: ${err.toString()}`, "WARNING");
        }
    }

    // Track successful buckets and their record counts
    const successfulBuckets = [];

    // Create temporary tables for each bucket
    logStep("Analyzing data distribution across bucket types", "IN_PROGRESS");
    for (const [bucketName, needsRefId, needsInternalSeries, needsEpisodeNum, needsSeasonNumber] of bucketCategories) {
        let conditions = [baseConditions];

        // Add specific conditions based on bucket type
        if (bucketName === "FULL_DATA") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''");
            conditions.push("REGEXP_LIKE(episode_number, '^[0-9]+$')");
            conditions.push("REGEXP_LIKE(season_number, '^[0-9]+$')");
        }
        else if (bucketName === "REF_ID_SERIES") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
        }
        else if (bucketName === "REF_ID_ONLY") {
            conditions.push("ref_id IS NOT NULL AND TRIM(ref_id) != ''");
            conditions.push("(internal_series IS NULL OR TRIM(internal_series) = '')");
        }
        else if (bucketName === "SERIES_SEASON_EPISODE") {
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("episode_number IS NOT NULL AND TRIM(episode_number) != ''");
            conditions.push("season_number IS NOT NULL AND TRIM(season_number) != ''");
            conditions.push("REGEXP_LIKE(episode_number, '^[0-9]+$')");
            conditions.push("REGEXP_LIKE(season_number, '^[0-9]+$')");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '')");
        }
        else if (bucketName === "SERIES_ONLY") {
            conditions.push("internal_series IS NOT NULL AND TRIM(internal_series) != ''");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '')");
            conditions.push("((episode_number IS NULL OR TRIM(episode_number) = '' OR NOT REGEXP_LIKE(episode_number, '^[0-9]+$')) OR (season_number IS NULL OR TRIM(season_number) = '' OR NOT REGEXP_LIKE(season_number, '^[0-9]+$')))");
        }
        else if (bucketName === "TITLE_ONLY") {
            conditions.push("platform_content_name IS NOT NULL AND TRIM(platform_content_name) != ''");
            conditions.push("(ref_id IS NULL OR TRIM(ref_id) = '') AND (internal_series IS NULL OR TRIM(internal_series) = '')");
        }

        // Check for any records matching this bucket before creating the temp table
        const checkSql = `
            SELECT COUNT(*) AS MATCH_COUNT
            FROM ${viewershipTable}
            WHERE ${conditions.join(" AND ")}`;

        let matchCount = 0;
        try {
            const checkResult = snowflake.execute({sqlText: checkSql});
            if (checkResult.next()) {
                matchCount = checkResult.getColumnValue('MATCH_COUNT');
            }

            logStep(`Found ${matchCount} records matching criteria for bucket ${bucketName}`, "INFO");

            if (matchCount === 0) {
                logStep(`Skipping bucket ${bucketName} - no matching records`, "INFO");
                continue;
            }

            // Add to successful buckets if it contains records
            successfulBuckets.push({
                name: bucketName,
                rowCount: matchCount
            });

        } catch (err) {
            logStep(`Error checking for matches in bucket ${bucketName}: ${err.toString()}`, "ERROR");
            continue;
        }
    }

    // If no successful buckets were created, return early
    if (successfulBuckets.length === 0) {
        logStep("No buckets with records were created, nothing to process", "COMPLETED", "0");
        return "No buckets with records were created, nothing to process";
    }

    // Now process each successful bucket with the appropriate matching strategy
    let totalProcessed = 0;
    const bucketResults = [];

    // First, manually create the unmatched records table with all records
    const createUnmatchedSql = `
    CREATE OR REPLACE TABLE {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED AS
    SELECT DISTINCT id
    FROM {{STAGING_DB}}.public.platform_viewership
    WHERE ${baseConditions}
    `;

    snowflake.execute({sqlText: createUnmatchedSql});

    // Check the initial count of records in the unmatched table
    const initialCountSql = `
    SELECT COUNT(*) AS INITIAL_COUNT
    FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;

    const initialCountResult = snowflake.execute({sqlText: initialCountSql});
    let initialCount = 0;
    if (initialCountResult.next()) {
        initialCount = initialCountResult.getColumnValue('INITIAL_COUNT');
    }

    logStep(`Initial count of records to process: ${initialCount}`, "INFO");

    // Process each bucket type in order
    for (const bucketType of bucketOrder) {
        // Check how many unmatched records we have left
        const checkUnmatchedSql = `
        SELECT COUNT(*) AS UNMATCHED_COUNT
        FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;

        const unmatchedResult = snowflake.execute({sqlText: checkUnmatchedSql});
        let unmatchedCount = 0;
        if (unmatchedResult.next()) {
            unmatchedCount = unmatchedResult.getColumnValue('UNMATCHED_COUNT');
        }

        logStep(`Processing ${bucketType} - Found ${unmatchedCount} unmatched records`, "INFO");

        if (unmatchedCount === 0) {
            logStep(`No unmatched records left to process for ${bucketType}`, "INFO");
            continue; // Skip to the next bucket type
        }

        // Create temporary bucket with unmatched records
        const bucketTableName = `TEMP_${platformArg.toUpperCase()}_${bucketType}_BUCKET`;

        // Create the bucket - with modified filtering to prevent full data records from flowing to less stringent procedures
        let createBucketSql;
        if (bucketType === "FULL_DATA") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''
            AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
            AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
            `;
        } else if (bucketType === "REF_ID_SERIES") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            `;
        } else if (bucketType === "REF_ID_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.ref_id IS NOT NULL AND TRIM(v.ref_id) != ''
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        } else if (bucketType === "SERIES_SEASON_EPISODE") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND v.episode_number IS NOT NULL AND TRIM(v.episode_number) != ''
            AND v.season_number IS NOT NULL AND TRIM(v.season_number) != ''
            AND REGEXP_LIKE(v.episode_number, '^[0-9]+$')
            AND REGEXP_LIKE(v.season_number, '^[0-9]+$')
            `;
        } else if (bucketType === "SERIES_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
            AND v.internal_series IS NOT NULL AND TRIM(v.internal_series) != ''
            AND ((v.episode_number IS NULL OR TRIM(v.episode_number) = '' OR NOT REGEXP_LIKE(v.episode_number, '^[0-9]+$'))
                 OR (v.season_number IS NULL OR TRIM(v.season_number) = '' OR NOT REGEXP_LIKE(v.season_number, '^[0-9]+$')))
            `;
        }
        else if (bucketType === "TITLE_ONLY") {
            createBucketSql = `
            CREATE OR REPLACE TEMPORARY TABLE {{UPLOAD_DB}}.PUBLIC.${bucketTableName} AS
            SELECT u.id
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            JOIN {{STAGING_DB}}.public.platform_viewership v ON u.id = v.id
            WHERE v.platform = '${platformArg}'
            AND v.platform_content_name IS NOT NULL AND TRIM(v.platform_content_name) != ''
            AND (v.ref_id IS NULL OR TRIM(v.ref_id) = '')
            AND (v.internal_series IS NULL OR TRIM(v.internal_series) = '')
            `;
        }
        snowflake.execute({sqlText: createBucketSql});

        // Count the actual records in the bucket
        const countActualSql = `SELECT COUNT(*) AS ACTUAL_COUNT FROM {{UPLOAD_DB}}.PUBLIC.${bucketTableName}`;
        const actualCountResult = snowflake.execute({sqlText: countActualSql});
        let actualCount = 0;
        if (actualCountResult.next()) {
            actualCount = actualCountResult.getColumnValue('ACTUAL_COUNT');
        }
        logStep(`${bucketType}: Starting to process ${actualCount} records`, "INFO");

        // If actual data count in bucket is zero, we don't need to run bucket procs
        if (!actualCount) {
            logStep(`${bucketType}: Skipping - no records need processing`, "INFO");
            continue;
        }

        // Process this bucket with filename parameter - call the GENERIC sub-procedure
        const processSql = `
        CALL {{UPLOAD_DB}}.public.process_viewership_${bucketType.toLowerCase()}_generic(
            '${platformArg}'
            ${filenameArg ? `, '${filenameArg.replace(/'/g, "''")}'` : ', NULL'}
        )`;

        try {
            const bucketResult = snowflake.execute({sqlText: processSql});
            let bucketResultStr = "";
            if (bucketResult.next()) {
                bucketResultStr = bucketResult.getColumnValue(1);
            }

            // Extract rows affected from the result
            const bucketRowsMatch = bucketResultStr.match(/updated (\d+) rows/);
            const bucketRowsAffected = bucketRowsMatch ? bucketRowsMatch[1] : "0";
            const bucketRowsCount = parseInt(bucketRowsAffected, 10);

            logStep(`${bucketType}: Successfully updated ${bucketRowsAffected} records`, "SUCCESS", bucketRowsAffected);

            // IMPORTANT: Always count this bucket's results, even if zero rows were affected
            bucketResults.push(`${bucketType}: ${bucketRowsAffected} records`);
            totalProcessed += bucketRowsCount;

            // Update the unmatched records table to remove any we just matched
            const updateUnmatchedSql = `
            DELETE FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED u
            WHERE EXISTS (
                SELECT 1
                FROM {{STAGING_DB}}.public.platform_viewership v
                WHERE v.id = u.id
                AND v.platform = '${platformArg}'
                AND v.content_provider IS NOT NULL
            )`;

            snowflake.execute({sqlText: updateUnmatchedSql});

            // Count how many records remain in the unmatched table
            const remainingUnmatchedSql = `
            SELECT COUNT(*) AS REMAINING_UNMATCHED
            FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;

            const remainingResult = snowflake.execute({sqlText: remainingUnmatchedSql});
            let remainingCount = 0;
            if (remainingResult.next()) {
                remainingCount = remainingResult.getColumnValue('REMAINING_UNMATCHED');
            }

            logStep(`After ${bucketType} processing, ${remainingCount} records still need processing by other strategies`, "INFO");

        } catch (err) {
            logStep(`Error processing ${bucketType} bucket: ${err.toString()}`, "ERROR");
        }

        // Clean up this bucket
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS {{UPLOAD_DB}}.PUBLIC.${bucketTableName}`});
    }

    // Clean up all temporary bucket tables
    for (const bucketType of bucketOrder) {
        try {
            snowflake.execute({sqlText: `DROP TABLE IF EXISTS {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_${bucketType}_BUCKET`});
        } catch (err) {
            logStep(`Warning: Failed to drop temporary table for ${bucketType}`, "WARNING", "0", err.toString());
        }
    }

    // Calculate the total number of unmatched records
    const unmatchedCount = initialCount - totalProcessed;
    logStep(`Total records: ${initialCount}, Processed: ${totalProcessed}, Unmatched: ${unmatchedCount}`, "INFO");

    // Check for any records that remained unmatched
    try {
        const finalUnmatchedSql = `
        SELECT COUNT(*) AS FINAL_UNMATCHED
        FROM {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`;

        const finalResult = snowflake.execute({sqlText: finalUnmatchedSql});
        let finalUnmatchedCount = 0;
        if (finalResult.next()) {
            finalUnmatchedCount = finalResult.getColumnValue('FINAL_UNMATCHED');
        }

        if (finalUnmatchedCount > 0) {
            logStep(`FINAL RESULT: ${finalUnmatchedCount} records could not be processed by any strategy`, "WARNING");
        }

        // Clean up the unmatched records table
        snowflake.execute({sqlText: `DROP TABLE IF EXISTS {{UPLOAD_DB}}.PUBLIC.TEMP_${platformArg.toUpperCase()}_UNMATCHED`});
        logStep("Dropped unmatched records table", "INFO");
    } catch (err) {
        logStep(`Warning: Failed to finalize unmatched records: ${err.toString()}`, "WARNING", "0", err.toString());
    }

    const totalExecutionTime = (new Date() - startTime) / 1000;
    const completionMessage = filenameArg
        ? `Processing completed for platform ${platformArg}, filename ${filenameArg} in ${totalExecutionTime} seconds. Total records processed: ${totalProcessed}`
        : `Processing completed for platform ${platformArg} in ${totalExecutionTime} seconds. Total records processed: ${totalProcessed}`;

    logStep(completionMessage, "COMPLETED", totalProcessed.toString());

    // IMPORTANT: Always return a success message if totalProcessed > 0
    if (totalProcessed > 0) {
        return `FINAL SUMMARY: Successfully updated ${totalProcessed} total records. Breakdown: ${bucketResults.join(", ")}`;
    } else {
        return `FINAL SUMMARY: No records were updated by any strategy.`;
    }
}
catch (err) {
    return `Error in analyze_and_process_viewership_data_generic: ${err.message}`;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.analyze_and_process_viewership_data_generic(VARCHAR, VARCHAR) TO ROLE web_app;

-- Note: This procedure requires the following generic sub-procedures to be created:
-- - {{UPLOAD_DB}}.public.process_viewership_full_data_generic
-- - {{UPLOAD_DB}}.public.process_viewership_ref_id_series_generic
-- - {{UPLOAD_DB}}.public.process_viewership_ref_id_only_generic
-- - {{UPLOAD_DB}}.public.process_viewership_series_season_episode_generic
-- - {{UPLOAD_DB}}.public.process_viewership_series_only_generic
-- - {{UPLOAD_DB}}.public.process_viewership_title_only_generic


-- ==============================================================================
-- PHASE 3 PROCEDURE 1: move_data_to_final_table_dynamic_generic
-- ==============================================================================
-- Moves processed data from {{STAGING_DB}}.public.platform_viewership to
-- {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}.
-- Handles both viewership and revenue data types.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.move_data_to_final_table_dynamic_generic("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
let sql_command;
try {
    const platform = PLATFORM.toLowerCase();
    const type = TYPE.toLowerCase();
    const lowerFilename = FILENAME.toLowerCase();

    // Log procedure start
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Starting procedure - Platform: ${PLATFORM}, Type: ${TYPE}, Filename: ${FILENAME}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
    });

    // if type is viewership or viewership + revenue
    if (type.includes("viewership")) {
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Starting viewership INSERT`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });

        sql_command = `
            INSERT INTO {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}(viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, minutes, hours, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views, platform_partner_name, platform_channel_name, platform_territory)
            SELECT id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), sum(tot_mov), sum(tot_hov), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Viewership', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views, platform_partner_name, platform_channel_name, platform_territory
            FROM {{STAGING_DB}}.public.platform_viewership
            WHERE platform = '${PLATFORM}'
            AND deal_parent is not null
            AND processed is null
            AND ref_id is not null
            AND asset_series is not null
            AND tot_mov is not null
            AND tot_hov is not null
            AND LOWER(filename) = '${lowerFilename}'
            GROUP BY all;
        `;

        const stmt = snowflake.createStatement({sqlText: sql_command});
        stmt.execute();
        const rowsAffected = stmt.getNumRowsAffected();

        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Viewership INSERT completed. Rows affected: ${rowsAffected}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });
    }

    // if type is revenue or viewership + revenue
    if (type.includes("revenue")) {
        // Check if there are records that should match revenue criteria first
        const revenueCheckQuery = `
            SELECT COUNT(*) FROM {{STAGING_DB}}.public.platform_viewership
            WHERE platform = '${PLATFORM}'
            AND deal_parent is not null
            AND processed is null
            AND ref_id is not null
            AND asset_series is not null
            AND revenue is not null
            AND revenue > 0
            AND LOWER(filename) = '${lowerFilename}'
        `;
        const revenueCheckResult = snowflake.execute({sqlText: revenueCheckQuery});
        let revenueCount = 0;
        if (revenueCheckResult.next()) {
            revenueCount = revenueCheckResult.getColumnValue(1);
        }

        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [`Revenue records found: ${revenueCount}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
        });

        if (revenueCount > 0) {
            sql_command = `
                insert into {{ASSETS_DB}}.public.{{EPISODE_DETAILS_TABLE}}(
                    viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views,
                    register_name, payment_amount, revenue_amount, payment_date, payment_type, payment_title, payment_description, payment_department, payment_adjustment, payment_quarter, payment_year, payment_month, payment_support_category, payment_filename
                )
                select
                    id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Revenue', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views,
                    CONCAT(partner, ' Revenue ', territory), revenue, revenue, year_month_day, '', '', '', '', 'False', quarter, year, month, 'Revenue', filename
                from {{STAGING_DB}}.public.platform_viewership
                WHERE platform = '${PLATFORM}'
                AND deal_parent is not null
                AND processed is null
                AND ref_id is not null
                AND asset_series is not null
                AND revenue is not null
                AND revenue > 0
                AND LOWER(filename) = '${lowerFilename}'
                GROUP BY ALL
            `;

            snowflake.execute({
                sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`Executing revenue INSERT for ${revenueCount} records`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });

            const revenueStmt = snowflake.createStatement({sqlText: sql_command});
            revenueStmt.execute();
            const revenueRowsAffected = revenueStmt.getNumRowsAffected();

            snowflake.execute({
                sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`Revenue INSERT completed. Rows affected: ${revenueRowsAffected}`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });
        } else {
            snowflake.execute({
                sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
                binds: [`No revenue records found - skipping revenue INSERT`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
            });
        }
    }

    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Procedure completed successfully`, 'move_data_to_final_table_dynamic_generic', PLATFORM]
    });

    return "Data moved successfully.";

} catch (error) {
    const errorMessage = "Error in sql query: " + sql_command + " error: " + error;
    console.error("Error executing SQL command:", error.message);
    snowflake.execute({
        sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [`PROCEDURE FAILED: ${errorMessage}`, 'move_data_to_final_table_dynamic_generic', PLATFORM, error.message]
    });
    return "Error executing SQL command: " + error.message;
}
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.move_data_to_final_table_dynamic_generic(VARCHAR, VARCHAR, VARCHAR) TO ROLE web_app;

-- ==============================================================================
-- PHASE 3 PROCEDURE 2: handle_final_insert_dynamic_generic
-- ==============================================================================
-- Orchestrates Phase 3 execution:
-- 1. Validates data (if validation procedure exists)
-- 2. Moves data to final table
-- 3. Updates phase to 3
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.handle_final_insert_dynamic_generic("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR DEFAULT null)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function executeStoredProcedure(procName, args) {
        try {
            var sqlCommand = `CALL ` + procName + (args.length > 0 ? `(${args.map(arg => arg === null ? 'NULL' : `'${String(arg).replace(/'/g, "''")}'`).join(',')})` : '()') + `;`;
            var result = snowflake.createStatement({sqlText: sqlCommand}).execute();
            if (result.next()) {
                return { success: true, message: procName + " executed successfully.", data: result.getColumnValue(1) };
            }
            return { success: true, message: procName + " executed successfully." };
        } catch (err) {
            // Logging the error to the console
            console.error("Error executing " + procName + ": " + err.message);
            // Returning error details for further handling
            return { success: false, message: "Error executing " + procName + ": " + err.message };
        }
    }

    function sendValidationErrorEmail(platform, filename, validationData) {
        try {
            // Format date for email
            var today = new Date();
            var dateStr = today.toISOString().split("T")[0];

            // Build HTML email content
            var html_message = '<html><body style="font-family: Arial, sans-serif;">';
            html_message += '<h2 style="color: #FF0000;">🚨 URGENT! Validation Errors in Viewership Data</h2>';
            html_message += '<p><strong>Platform:</strong> ' + platform + '</p>';
            if (filename) {
                html_message += '<p><strong>Filename:</strong> ' + filename + '</p>';
            }
            html_message += '<p><strong>Date:</strong> ' + dateStr + '</p>';
            html_message += '<p><strong>Total Records Checked:</strong> ' + validationData.validationCount + '</p>';
            html_message += '<p><strong>Total Errors Found:</strong> ' + validationData.errors.length + '</p>';

            // Show error summary
            html_message += "<h3>Error Summary</h3>";

            // Group errors by type
            var errorTypes = {};
            validationData.errors.forEach(function(err) {
                if (!errorTypes[err.error]) {
                    errorTypes[err.error] = 0;
                }
                errorTypes[err.error]++;
            });

            html_message += "<ul>";
            for (var errorType in errorTypes) {
                html_message += '<li>' + errorType + ': ' + errorTypes[errorType] + ' records</li>';
            }
            html_message += "</ul>";

            // Show detailed errors (limit to first 50 for email size)
            var errorLimit = Math.min(50, validationData.errors.length);
            html_message += '<h3>Detailed Errors (showing first ' + errorLimit + ' of ' + validationData.errors.length + ')</h3>';
            html_message += '<table style="border-collapse: collapse; width: 100%;">';
            html_message += '<thead>';
            html_message += '<tr style="background-color: #f2f2f2;">';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Record ID</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Error Type</th>';
            html_message += '<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">Details</th>';
            html_message += '</tr>';
            html_message += '</thead>';
            html_message += '<tbody>';

            // Add rows for each error (up to limit)
            for (var i = 0; i < errorLimit; i++) {
                var err = validationData.errors[i];
                html_message += '<tr>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + err.id + '</td>';
                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + err.error + '</td>';

                // Build details column based on error fields
                var details = "";
                for (var key in err) {
                    if (key !== 'id' && key !== 'error') {
                        details += '<strong>' + key + ':</strong> ' + err[key] + '<br>';
                    }
                }

                html_message += '<td style="border: 1px solid #ddd; padding: 8px;">' + details + '</td>';
                html_message += '</tr>';
            }

            html_message += '</tbody></table>';
            html_message += '<p><strong>Action Required:</strong> Please fix these validation errors before attempting to insert the data again.</p>';
            html_message += '</body></html>';

            // Send email using the monitoring procedure
            snowflake.execute({
                sqlText: `
                CALL SYSTEM$SEND_EMAIL(
                    'SNOWFLAKE_EMAIL_SENDER',
                    'tayloryoung@mvmediasales.com, data@nosey.com',
                    ?,
                    ?,
                    'text/html'
                )`,
                binds: [
                    "🚨 URGENT: Viewership Validation Errors - " + platform + (filename ? ' - ' + filename : ''),
                    html_message
                ]
            });

            return { success: true, message: "Validation error email sent." };
        } catch (err) {
            console.error("Error sending validation error email: " + err.message);
            return { success: false, message: "Error sending validation error email: " + err.message };
        }
    }

    // First, run the validation procedure (if it exists)
    // Note: validate_viewership_for_insert may need to be created or updated for generic architecture
    try {
        var validationResult = executeStoredProcedure("{{UPLOAD_DB}}.public.validate_viewership_for_insert", [PLATFORM, FILENAME]);

        if (validationResult.success && validationResult.data) {
            // Check validation results
            var validationData = validationResult.data;
            if (!validationData.valid) {
                // Send email notification about validation errors
                var emailResult = sendValidationErrorEmail(PLATFORM, FILENAME, validationData);

                // Log the validation failure and return - THIS TERMINATES THE PROCESS
                var errorCount = validationData.errors.length;
                var recordCount = validationData.validationCount;
                var errorMessage = `Validation failed: ${errorCount} errors found in ${recordCount} records. Email notification sent.`;
                console.error(errorMessage);

                // Return detailed error message
                return errorMessage;
            }

            // Log validation results with matched/unmatched breakdown
            var matched = validationData.matchedCount || validationData.validationCount;
            var unmatched = validationData.unmatchedCount || 0;
            console.log(`Validation passed: ${validationData.validationCount} total records, ${matched} matched, ${unmatched} unmatched (expected).`);
        }
    } catch (validationErr) {
        // If validation procedure doesn't exist or fails, log and continue
        console.log("Validation step skipped or failed: " + validationErr.message);
    }

    // If validation passes or is skipped, proceed with the regular procedure sequence
    var procedures = [
        { name: `{{UPLOAD_DB}}.public.move_data_to_final_table_dynamic_generic`, args: [PLATFORM, TYPE, FILENAME] },
        { name: "{{UPLOAD_DB}}.public.set_phase_generic", args: [PLATFORM, 3, FILENAME] },
    ];

    for (var i = 0; i < procedures.length; i++) {
        var proc = procedures[i];
        var result = executeStoredProcedure(proc.name, proc.args);
        if (!result.success) {
            // If a procedure fails, log the failure and halt further execution
            console.error(result.message);
            return result.message;
        }
    }

    console.log("All procedures executed successfully.");
    return "All procedures executed successfully.";
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.handle_final_insert_dynamic_generic(STRING, STRING, STRING) TO ROLE web_app;

-- ==============================================================================
-- DEPLOYMENT COMPLETE
-- ==============================================================================
-- All generic procedures have been created.
--
-- Next steps:
-- 1. Deploy Lambda with updated procedure names
-- 2. Test full pipeline from Phase 0 through Phase 3
-- ==============================================================================
