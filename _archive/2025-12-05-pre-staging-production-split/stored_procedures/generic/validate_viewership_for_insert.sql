CREATE OR REPLACE PROCEDURE "VALIDATE_VIEWERSHIP_FOR_INSERT"("PLATFORM" VARCHAR, "FILENAME" VARCHAR, "DATA_TYPE" VARCHAR DEFAULT 'Viewership')
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    try {
        const upperPlatform = PLATFORM.toUpperCase();
        const lowerFilename = FILENAME.toLowerCase();
        const lowerDataType = (DATA_TYPE || ''Viewership'').toLowerCase();
        // Query records from test_staging (where Lambda processes data)
        // Check records that are ready for final insert (phase 2, not processed)
        const countQuery = `
            SELECT COUNT(*) as total_count
            FROM TEST_STAGING.public.platform_viewership
            WHERE UPPER(platform) = ''${upperPlatform}''
              AND LOWER(filename) = ''${lowerFilename}''
              AND processed IS NULL
              AND phase = ''2''
        `;
        const countStmt = snowflake.createStatement({sqlText: countQuery});
        const countResult = countStmt.execute();
        countResult.next();
        const totalCount = countResult.getColumnValue(''TOTAL_COUNT'');
        // Query for BLOCKING validation errors (data quality issues)
        // NOTE: Missing ref_id/asset_series are EXPECTED (unmatched content) and handled separately
        // Build validation query based on data type
        let validationQuery = '''';
        if (lowerDataType.includes(''revenue'')) {
            // Revenue validation - check for revenue column instead of viewership metrics
            // Note: week and day may be NULL for revenue-by-episode files (only month/year)
            validationQuery = `
                SELECT
                    id,
                    CASE
                        WHEN deal_parent IS NULL THEN ''Missing deal_parent''
                        WHEN revenue IS NULL THEN ''Missing revenue''
                        WHEN revenue <= 0 THEN ''Invalid revenue (must be > 0)''
                        WHEN year IS NULL THEN ''Missing year''
                        WHEN quarter IS NULL THEN ''Missing quarter''
                        WHEN year_month_day IS NULL THEN ''Missing year_month_day''
                        ELSE ''Unknown error''
                    END as error,
                    deal_parent,
                    ref_id,
                    asset_series,
                    revenue,
                    year,
                    quarter,
                    year_month_day,
                    platform_content_name,
                    platform_series
                FROM TEST_STAGING.public.platform_viewership
                WHERE UPPER(platform) = ''${upperPlatform}''
                  AND LOWER(filename) = ''${lowerFilename}''
                  AND processed IS NULL
                  AND phase = ''2''
                  AND (
                      deal_parent IS NULL
                      OR revenue IS NULL
                      OR revenue <= 0
                      OR year IS NULL
                      OR quarter IS NULL
                      OR year_month_day IS NULL
                  )
                LIMIT 100
            `;
        } else {
            // Viewership validation - check for BLOCKING data quality issues ONLY
            // NOTE: Missing asset_series/content_provider are NOT blocking (unmatched content handled by Lambda)
            validationQuery = `
                SELECT
                    id,
                    CASE
                        WHEN tot_hov IS NULL THEN ''Missing tot_hov''
                        WHEN tot_mov IS NULL THEN ''Missing tot_mov''
                        WHEN year IS NULL THEN ''Missing year''
                        WHEN quarter IS NULL THEN ''Missing quarter''
                        ELSE ''Unknown error''
                    END as error,
                    deal_parent,
                    ref_id,
                    asset_series,
                    asset_title,
                    content_provider,
                    tot_mov,
                    tot_hov,
                    year,
                    quarter,
                    platform_content_name,
                    platform_series
                FROM TEST_STAGING.public.platform_viewership
                WHERE UPPER(platform) = ''${upperPlatform}''
                  AND LOWER(filename) = ''${lowerFilename}''
                  AND processed IS NULL
                  AND phase = ''2''
                  AND (
                      tot_hov IS NULL
                      OR tot_mov IS NULL
                      OR year IS NULL
                      OR quarter IS NULL
                  )
                LIMIT 100
            `;
        }
        // Separate query for unmatched content (expected, not blocking)
        const unmatchedQuery = `
            SELECT COUNT(*) as unmatched_count
            FROM TEST_STAGING.public.platform_viewership
            WHERE UPPER(platform) = ''${upperPlatform}''
              AND LOWER(filename) = ''${lowerFilename}''
              AND processed IS NULL
              AND phase = ''2''
              AND (ref_id IS NULL OR asset_series IS NULL)
        `;
        const validationStmt = snowflake.createStatement({sqlText: validationQuery});
        const validationResult = validationStmt.execute();
        // Collect BLOCKING errors only
        const errors = [];
        while (validationResult.next()) {
            const errorObj = {
                id: validationResult.getColumnValue(''ID''),
                error: validationResult.getColumnValue(''ERROR''),
                deal_parent: validationResult.getColumnValue(''DEAL_PARENT''),
                ref_id: validationResult.getColumnValue(''REF_ID''),
                asset_series: validationResult.getColumnValue(''ASSET_SERIES''),
                platform_content_name: validationResult.getColumnValue(''PLATFORM_CONTENT_NAME''),
                platform_series: validationResult.getColumnValue(''PLATFORM_SERIES'')
            };
            // Add type-specific fields
            if (lowerDataType.includes(''revenue'')) {
                errorObj.revenue = validationResult.getColumnValue(''REVENUE'');
                errorObj.year = validationResult.getColumnValue(''YEAR'');
                errorObj.quarter = validationResult.getColumnValue(''QUARTER'');
                errorObj.year_month_day = validationResult.getColumnValue(''YEAR_MONTH_DAY'');
            } else {
                errorObj.tot_mov = validationResult.getColumnValue(''TOT_MOV'');
                errorObj.tot_hov = validationResult.getColumnValue(''TOT_HOV'');
                errorObj.year = validationResult.getColumnValue(''YEAR'');
                errorObj.quarter = validationResult.getColumnValue(''QUARTER'');
            }
            errors.push(errorObj);
        }
        // Get count of unmatched content (non-blocking)
        const unmatchedStmt = snowflake.createStatement({sqlText: unmatchedQuery});
        const unmatchedResult = unmatchedStmt.execute();
        unmatchedResult.next();
        const unmatchedCount = unmatchedResult.getColumnValue(''UNMATCHED_COUNT'');
        // Calculate matched records
        const matchedCount = totalCount - unmatchedCount;
        // Build result object
        // Validation passes if no BLOCKING errors (unmatched content is OK)
        // If totalCount is 0, records were already processed/moved - that''s valid too
        const result = {
            valid: errors.length === 0,
            validationCount: totalCount,
            matchedCount: matchedCount,
            unmatchedCount: unmatchedCount,
            errors: errors
        };
        // Log the validation result
        snowflake.execute({
            sqlText: "INSERT INTO UPLOAD_DB.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
            binds: [
                `Validation completed: ${totalCount} records checked, ${errors.length} blocking errors, ${unmatchedCount} unmatched content (expected)`,
                ''validate_viewership_for_insert'',
                PLATFORM
            ]
        });
        return result;
    } catch (err) {
        const errorMessage = "Error in validate_viewership_for_insert: " + err.message;
        // Log the error
        snowflake.execute({
            sqlText: "INSERT INTO UPLOAD_DB.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
            binds: [errorMessage, ''validate_viewership_for_insert'', PLATFORM, err.message]
        });
        // Return error result
        return {
            valid: false,
            validationCount: 0,
            errors: [{
                id: null,
                error: ''Validation procedure failed: '' + err.message
            }]
        };
    }
';