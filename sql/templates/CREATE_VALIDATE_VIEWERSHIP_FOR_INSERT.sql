-- ==============================================================================
-- CREATE: validate_viewership_for_insert
-- ==============================================================================
-- Validates records in test_staging before inserting to final table
-- Returns JSON with validation results
-- ==============================================================================

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.validate_viewership_for_insert(
    platform VARCHAR,
    filename VARCHAR
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        const upperPlatform = PLATFORM.toUpperCase();
        const lowerFilename = FILENAME.toLowerCase();

        // Query records from test_staging (where Lambda processes data)
        // Check records that are ready for final insert (phase 2, not processed)
        const countQuery = `
            SELECT COUNT(*) as total_count
            FROM {{STAGING_DB}}.public.platform_viewership
            WHERE UPPER(platform) = '${upperPlatform}'
              AND LOWER(filename) = '${lowerFilename}'
              AND processed IS NULL
              AND phase = '2'
        `;

        const countStmt = snowflake.createStatement({sqlText: countQuery});
        const countResult = countStmt.execute();
        countResult.next();
        const totalCount = countResult.getColumnValue('TOTAL_COUNT');

        // Query for BLOCKING validation errors (data quality issues)
        // NOTE: Missing ref_id/asset_series are EXPECTED (unmatched content) and handled separately
        const validationQuery = `
            SELECT
                id,
                CASE
                    WHEN deal_parent IS NULL THEN 'Missing deal_parent'
                    WHEN tot_mov IS NULL THEN 'Missing tot_mov'
                    WHEN tot_hov IS NULL THEN 'Missing tot_hov'
                    WHEN week IS NULL THEN 'Missing week'
                    WHEN day IS NULL THEN 'Missing day'
                    ELSE 'Unknown error'
                END as error,
                deal_parent,
                ref_id,
                asset_series,
                tot_mov,
                tot_hov,
                week,
                day,
                platform_content_name,
                platform_series
            FROM {{STAGING_DB}}.public.platform_viewership
            WHERE UPPER(platform) = '${upperPlatform}'
              AND LOWER(filename) = '${lowerFilename}'
              AND processed IS NULL
              AND phase = '2'
              AND (
                  deal_parent IS NULL
                  OR tot_mov IS NULL
                  OR tot_hov IS NULL
                  OR week IS NULL
                  OR day IS NULL
              )
            LIMIT 100
        `;

        // Separate query for unmatched content (expected, not blocking)
        const unmatchedQuery = `
            SELECT COUNT(*) as unmatched_count
            FROM {{STAGING_DB}}.public.platform_viewership
            WHERE UPPER(platform) = '${upperPlatform}'
              AND LOWER(filename) = '${lowerFilename}'
              AND processed IS NULL
              AND phase = '2'
              AND (ref_id IS NULL OR asset_series IS NULL)
        `;

        const validationStmt = snowflake.createStatement({sqlText: validationQuery});
        const validationResult = validationStmt.execute();

        // Collect BLOCKING errors only
        const errors = [];
        while (validationResult.next()) {
            errors.push({
                id: validationResult.getColumnValue('ID'),
                error: validationResult.getColumnValue('ERROR'),
                deal_parent: validationResult.getColumnValue('DEAL_PARENT'),
                ref_id: validationResult.getColumnValue('REF_ID'),
                asset_series: validationResult.getColumnValue('ASSET_SERIES'),
                tot_mov: validationResult.getColumnValue('TOT_MOV'),
                tot_hov: validationResult.getColumnValue('TOT_HOV'),
                week: validationResult.getColumnValue('WEEK'),
                day: validationResult.getColumnValue('DAY'),
                platform_content_name: validationResult.getColumnValue('PLATFORM_CONTENT_NAME'),
                platform_series: validationResult.getColumnValue('PLATFORM_SERIES')
            });
        }

        // Get count of unmatched content (non-blocking)
        const unmatchedStmt = snowflake.createStatement({sqlText: unmatchedQuery});
        const unmatchedResult = unmatchedStmt.execute();
        unmatchedResult.next();
        const unmatchedCount = unmatchedResult.getColumnValue('UNMATCHED_COUNT');

        // Calculate matched records
        const matchedCount = totalCount - unmatchedCount;

        // Build result object
        // Validation passes if no BLOCKING errors (unmatched content is OK)
        const result = {
            valid: errors.length === 0 && totalCount > 0,
            validationCount: totalCount,
            matchedCount: matchedCount,
            unmatchedCount: unmatchedCount,
            errors: errors
        };

        // Log the validation result
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [
                `Validation completed: ${totalCount} records checked, ${errors.length} blocking errors, ${unmatchedCount} unmatched content (expected)`,
                'validate_viewership_for_insert',
                PLATFORM
            ]
        });

        return result;

    } catch (err) {
        const errorMessage = "Error in validate_viewership_for_insert: " + err.message;

        // Log the error
        snowflake.execute({
            sqlText: "INSERT INTO {{UPLOAD_DB}}.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
            binds: [errorMessage, 'validate_viewership_for_insert', PLATFORM, err.message]
        });

        // Return error result
        return {
            valid: false,
            validationCount: 0,
            errors: [{
                id: null,
                error: 'Validation procedure failed: ' + err.message
            }]
        };
    }
$$;

GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.public.validate_viewership_for_insert(VARCHAR, VARCHAR) TO ROLE web_app;
