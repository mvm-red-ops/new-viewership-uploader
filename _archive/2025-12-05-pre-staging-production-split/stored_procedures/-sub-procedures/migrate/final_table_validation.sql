GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT(VARCHAR, VARCHAR) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.VALIDATE_VIEWERSHIP_FOR_INSERT("PLATFORM" VARCHAR, "FILENAME" VARCHAR DEFAULT null)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Initialize result object
    var result = {
        valid: true,
        errors: [],
        validationCount: 0
    };

    try {
        // Build the platform-specific table name
        var viewershipTable = `test_staging.public.${PLATFORM}_viewership`;
        
        // 1. Check for NULL values in required fields
        var nullCheckQuery = `
            SELECT 
                id,
                'Missing required fields' AS error_message
            FROM 
                ${viewershipTable}
            WHERE 
                processed IS NULL
                AND (asset_series IS NULL OR asset_title IS NULL OR content_provider IS NULL)
                ${FILENAME ? `AND filename = '${FILENAME.replace(/'/g, "''")}'` : ''}
        `;
        
        var nullCheckStmt = snowflake.createStatement({sqlText: nullCheckQuery});
        var nullCheckResults = nullCheckStmt.execute();
        
        while (nullCheckResults.next()) {
            result.valid = false;
            result.errors.push({
                id: nullCheckResults.getColumnValue("id"),
                error: nullCheckResults.getColumnValue("error_message")
            });
        }
        
        // 2. Check that ref_id prefix matches series_code
        var refIdCheckQuery = `
            SELECT 
                id,
                ref_id,
                series_code,
                'ref_id prefix does not match series_code' AS error_message
            FROM 
                ${viewershipTable}
            WHERE 
                processed IS NULL
                AND SUBSTRING(ref_id, 1, 2) != series_code
                ${FILENAME ? `AND filename = '${FILENAME.replace(/'/g, "''")}'` : ''}
        `;
        
        var refIdCheckStmt = snowflake.createStatement({sqlText: refIdCheckQuery});
        var refIdCheckResults = refIdCheckStmt.execute();
        
        while (refIdCheckResults.next()) {
            result.valid = false;
            result.errors.push({
                id: refIdCheckResults.getColumnValue("id"),
                ref_id: refIdCheckResults.getColumnValue("ref_id"),
                series_code: refIdCheckResults.getColumnValue("series_code"),
                error: refIdCheckResults.getColumnValue("error_message")
            });
        }
        
        // 3. Join with series table to validate asset_series against metadata_master_cleaned_staging
        var seriesJoinQuery = `
            SELECT 
                v.id,
                v.asset_series,
                v.content_provider AS viewership_content_provider,
                s.content_provider AS series_content_provider,
                v.series_code AS viewership_series_code,
                s.series_code AS series_series_code,
                'Metadata mismatch with series table' AS error_message
            FROM 
                ${viewershipTable} v
            LEFT JOIN 
                metadata_master_cleaned_staging.public.series s 
                ON UPLOAD_DB.public.extract_primary_title(s.titles) = v.asset_series
            WHERE 
                v.processed IS NULL
                ${FILENAME ? `AND v.filename = '${FILENAME.replace(/'/g, "''")}'` : ''}
                AND (
                    s.content_provider IS NULL 
                    OR s.content_provider != v.content_provider
                    OR s.series_code != v.series_code
                )
        `;
        
        var seriesJoinStmt = snowflake.createStatement({sqlText: seriesJoinQuery});
        var seriesJoinResults = seriesJoinStmt.execute();
        
        while (seriesJoinResults.next()) {
            result.valid = false;
            result.errors.push({
                id: seriesJoinResults.getColumnValue("id"),
                asset_series: seriesJoinResults.getColumnValue("asset_series"),
                viewership_content_provider: seriesJoinResults.getColumnValue("viewership_content_provider"),
                series_content_provider: seriesJoinResults.getColumnValue("series_content_provider"),
                viewership_series_code: seriesJoinResults.getColumnValue("viewership_series_code"),
                series_series_code: seriesJoinResults.getColumnValue("series_series_code"),
                error: seriesJoinResults.getColumnValue("error_message")
            });
        }

        // Count how many records were validated
        var countQuery = `
            SELECT 
                COUNT(*) AS record_count
            FROM 
                ${viewershipTable}
            WHERE 
                processed IS NULL
                ${FILENAME ? `AND filename = '${FILENAME.replace(/'/g, "''")}'` : ''}
        `;
        
        var countStmt = snowflake.createStatement({sqlText: countQuery});
        var countResult = countStmt.execute();
        
        if (countResult.next()) {
            result.validationCount = countResult.getColumnValue("record_count");
        }
        
        // Log validation result
        console.log(`Validation completed: ${result.validationCount} records checked, ${result.errors.length} errors found.`);
        
    } catch (err) {
        // Handle any exceptions during validation
        result.valid = false;
        result.exception = err.message;
        console.error("Error during validation: " + err.message);
    }
    
    return result;
$$;