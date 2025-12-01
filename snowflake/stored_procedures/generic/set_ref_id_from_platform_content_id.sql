-- Set ref_id from platform_content_id by matching against known ref_ids in metadata
-- This procedure checks if platform_content_id contains a valid ref_id and copies it to the ref_id column

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.SET_REF_ID_FROM_PLATFORM_CONTENT_ID(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    const platformArg = PLATFORM;
    const filenameArg = FILENAME;

    console.log(`Setting ref_id from platform_content_id for ${platformArg} - ${filenameArg}`);

    // Step 1: Get all unique ref_ids from metadata
    // Using episode table as it has all the ref_ids we care about
    const getRefIdsSql = `
        SELECT DISTINCT ref_id
        FROM metadata_master_cleaned_staging.public.episode
        WHERE ref_id IS NOT NULL
          AND TRIM(ref_id) != ''
    `;

    console.log('Fetching all known ref_ids from metadata...');
    const refIdsResult = snowflake.execute({sqlText: getRefIdsSql});

    // Store ref_ids in an array
    const refIds = [];
    while (refIdsResult.next()) {
        refIds.push(refIdsResult.getColumnValue('REF_ID'));
    }

    console.log(`Found ${refIds.length} unique ref_ids in metadata`);

    if (refIds.length === 0) {
        return `No ref_ids found in metadata - skipping ref_id mapping`;
    }

    // Step 2: Update records where platform_content_id contains a known ref_id
    // We need to check if platform_content_id CONTAINS any of the known ref_ids
    // and extract the matching ref_id

    console.log('Updating ref_id column where platform_content_id contains known ref_ids...');

    let totalRowsUpdated = 0;

    // Process in batches to avoid SQL statement size limits
    const batchSize = 1000;
    for (let i = 0; i < refIds.length; i += batchSize) {
        const batch = refIds.slice(i, i + batchSize);

        // Create CASE statement to match and extract ref_ids
        const caseStatements = batch.map(refId => {
            const escapedRefId = refId.replace(/'/g, "''");
            return `WHEN CONTAINS(platform_content_id, '${escapedRefId}') THEN '${escapedRefId}'`;
        }).join('\n            ');

        const updateSql = `
            UPDATE test_staging.public.platform_viewership
            SET ref_id = CASE
                ${caseStatements}
            END
            WHERE UPPER(platform) = '${platformArg.toUpperCase()}'
              AND LOWER(filename) = '${filenameArg.toLowerCase()}'
              AND platform_content_id IS NOT NULL
              AND TRIM(platform_content_id) != ''
              AND (ref_id IS NULL OR TRIM(ref_id) = '')
              AND (${batch.map(refId => `CONTAINS(platform_content_id, '${refId.replace(/'/g, "''")}')`).join(' OR ')})
        `;

        const updateResult = snowflake.execute({sqlText: updateSql});
        const rowsUpdated = updateResult.getNumRowsAffected();
        totalRowsUpdated += rowsUpdated;

        console.log(`Processed batch ${Math.floor(i / batchSize) + 1}: ${rowsUpdated} rows updated`);
    }

    console.log(`✅ Set ref_id for ${totalRowsUpdated} records from platform_content_id`);
    return `Successfully set ref_id for ${totalRowsUpdated} records matching known ref_ids`;

} catch (err) {
    const errorMessage = "Error in set_ref_id_from_platform_content_id: " + err.message;
    console.error(errorMessage);

    // Log the error
    try {
        snowflake.execute({
            sqlText: "INSERT INTO UPLOAD_DB.PUBLIC.ERROR_LOG_TABLE (LOG_MESSAGE, PROCEDURE_NAME, PLATFORM) VALUES (?, ?, ?)",
            binds: [errorMessage, 'set_ref_id_from_platform_content_id', PLATFORM]
        });
    } catch (logErr) {
        console.error("Failed to log error: " + logErr.message);
    }

    throw new Error(errorMessage);
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.SET_REF_ID_FROM_PLATFORM_CONTENT_ID(STRING, STRING) TO ROLE web_app;
