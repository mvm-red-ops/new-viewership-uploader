-- Template for matching series from title using CONTAINS approach
-- This procedure:
-- 1. Searches for dictionary.public.series entries WITHIN PLATFORM_CONTENT_NAME
-- 2. Sets INTERNAL_SERIES to the matched series name
-- 3. Returns statistics (email notifications removed - this is an intermediate step)
--
-- This is safer than extraction because it relies on known series names from the dictionary
-- Note: Fallback matching happens after this via set_internal_series_generic

-- Template uses {{UPLOAD_DB}} for the database name
-- TABLE_NAME is passed as a parameter (e.g., 'platform_viewership')

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.public.SET_INTERNAL_SERIES_WITH_EXTRACTION(
    "TABLE_NAME" VARCHAR,
    "FILTER_COLUMN" VARCHAR,  -- e.g., 'filename' or 'platform'
    "FILTER_VALUE" VARCHAR     -- e.g., 'file123.csv' or 'Karamo'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var tableName = TABLE_NAME;
    var filterColumn = FILTER_COLUMN;
    var filterValue = FILTER_VALUE;

    // Function to match series using CONTAINS approach
    // Searches for dictionary series names within the PLATFORM_CONTENT_NAME
    function matchSeriesInTitle() {
        try {
            // Update records where dictionary series is found in the title
            var sqlText = `
                UPDATE ${tableName}
                SET internal_series = x.matched_series
                FROM (
                    SELECT
                        v.id,
                        s.series AS matched_series,
                        ROW_NUMBER() OVER (
                            PARTITION BY v.id
                            ORDER BY LENGTH(s.entry) DESC
                        ) AS match_rank
                    FROM ${tableName} v
                    JOIN dictionary.public.series s
                    ON (
                        -- Case-insensitive CONTAINS check
                        LOWER(v.platform_content_name) LIKE LOWER('%' || s.entry || '%')
                        OR LOWER(v.platform_content_name) LIKE LOWER('%' || s.series || '%')
                    )
                    WHERE v.${filterColumn} = '${filterValue}'
                    AND v.processed IS NULL
                    AND v.platform_content_name IS NOT NULL
                    AND (v.internal_series IS NULL OR v.internal_series = '')
                ) x
                WHERE ${tableName}.id = x.id
                AND x.match_rank = 1  -- Take longest matching series name to avoid false positives
            `;

            snowflake.execute({sqlText: sqlText});

            // Get count of matched rows
            var countQuery = snowflake.createStatement({
                sqlText: `
                    SELECT COUNT(*) AS matched_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND internal_series IS NOT NULL
                `
            }).execute();

            var matchedCount = 0;
            if (countQuery.next()) {
                matchedCount = countQuery.getColumnValue('MATCHED_COUNT');
            }

            return {
                success: true,
                matchedCount: matchedCount
            };
        } catch (err) {
            throw "Function matchSeriesInTitle failed: " + err;
        }
    }

    // Function to check for unmatched titles (after matching attempt)
    function checkForUnmatchedTitles() {
        try {
            var unmatchedCountQuery = snowflake.createStatement({
                sqlText: `
                    SELECT COUNT(*) AS unmatched_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND platform_content_name IS NOT NULL
                    AND (internal_series IS NULL OR internal_series = '')
                `
            }).execute();

            var unmatchedCount = 0;
            if (unmatchedCountQuery.next()) {
                unmatchedCount = unmatchedCountQuery.getColumnValue('UNMATCHED_COUNT');
            }

            if (unmatchedCount === 0) {
                return {
                    unmatchedCount: 0,
                    unmatchedRecords: []
                };
            }

            // Get sample unmatched records
            var unmatchedRecordsQuery = snowflake.createStatement({
                sqlText: `
                    SELECT DISTINCT
                        platform_content_name,
                        platform,
                        filename,
                        COUNT(*) OVER (PARTITION BY platform_content_name) AS occurrence_count
                    FROM ${tableName}
                    WHERE ${filterColumn} = '${filterValue}'
                    AND processed IS NULL
                    AND platform_content_name IS NOT NULL
                    AND (internal_series IS NULL OR internal_series = '')
                    ORDER BY occurrence_count DESC
                    LIMIT 100
                `
            }).execute();

            var unmatchedRecords = [];
            while (unmatchedRecordsQuery.next()) {
                unmatchedRecords.push({
                    platform: unmatchedRecordsQuery.getColumnValue('PLATFORM') || 'N/A',
                    filename: unmatchedRecordsQuery.getColumnValue('FILENAME') || 'N/A',
                    platform_content_name: unmatchedRecordsQuery.getColumnValue('PLATFORM_CONTENT_NAME') || 'N/A',
                    occurrence_count: unmatchedRecordsQuery.getColumnValue('OCCURRENCE_COUNT') || 0
                });
            }

            return {
                unmatchedCount: unmatchedCount,
                unmatchedRecords: unmatchedRecords
            };
        } catch (err) {
            throw "Function checkForUnmatchedTitles failed: " + err;
        }
    }

    // Email notification removed - this is an intermediate step
    // Final matching happens later in the pipeline via set_internal_series_generic
    // and analyze_and_process_viewership_data_generic

    try {
        // Step 1: Match series using CONTAINS approach
        var matchResults = matchSeriesInTitle();

        // Step 2: Check for unmatched titles (for statistics only)
        var unmatchedResults = checkForUnmatchedTitles();

        // Step 3: Get total count for return message
        var totalRecords = matchResults.matchedCount + unmatchedResults.unmatchedCount;

        // Step 4: Return success message with statistics
        // Note: Unmatched records here may be matched later by fallback procedures
        return "SET_INTERNAL_SERIES_WITH_EXTRACTION completed. " +
               "Total: " + totalRecords + ", " +
               "Matched by CONTAINS: " + matchResults.matchedCount + ", " +
               "Unmatched: " + unmatchedResults.unmatchedCount + " (fallback matching will run next)";
    } catch (err) {
        return "ERROR: " + err.toString();
    }
$$;
