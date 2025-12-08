-- Simplified Generic Normalize Data in Staging Procedure
-- For new architecture using platform_viewership table
-- Calls generic normalization procedures in sequence

CREATE OR REPLACE PROCEDURE {{UPLOAD_DB}}.PUBLIC.NORMALIZE_DATA_IN_STAGING(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    const platformArg = PLATFORM;
    const filenameArg = FILENAME;

    try {
        console.log(`Starting normalization for ${platformArg} - ${filenameArg}`);

        // Step 1: Set deal_parent (also sets partner, channel, territory, channel_id, territory_id)
        console.log('Step 1: Setting deal_parent...');
        snowflake.execute({
            sqlText: `CALL {{UPLOAD_DB}}.PUBLIC.SET_DEAL_PARENT_GENERIC(?, ?)`,
            binds: [platformArg, filenameArg]
        });

        // Step 2: Set ref_id from platform_content_id where applicable
        console.log('Step 2: Setting ref_id from platform_content_id...');
        snowflake.execute({
            sqlText: `CALL {{UPLOAD_DB}}.PUBLIC.SET_REF_ID_FROM_PLATFORM_CONTENT_ID(?, ?)`,
            binds: [platformArg, filenameArg]
        });

        // Step 3: Calculate viewership metrics (TOT_MOV from TOT_HOV or vice versa)
        console.log('Step 3: Calculating viewership metrics...');
        snowflake.execute({
            sqlText: `CALL {{UPLOAD_DB}}.PUBLIC.CALCULATE_VIEWERSHIP_METRICS(?, ?)`,
            binds: [platformArg, filenameArg]
        });

        // Step 4: Set date columns (week, day, quarter, year, month)
        console.log('Step 4: Setting date columns...');
        snowflake.execute({
            sqlText: `CALL {{UPLOAD_DB}}.PUBLIC.SET_DATE_COLUMNS_DYNAMIC(?, ?)`,
            binds: [platformArg, filenameArg]
        });

        // Step 5: Set phase to 1
        console.log('Step 5: Setting phase to 1...');
        snowflake.execute({
            sqlText: `CALL {{UPLOAD_DB}}.PUBLIC.SET_PHASE_GENERIC(?, ?, ?)`,
            binds: [platformArg, '1', filenameArg]
        });

        console.log(`✅ Normalization completed for ${platformArg} - ${filenameArg}`);
        return `Normalization completed successfully for ${platformArg} - ${filenameArg}`;

    } catch (err) {
        const errorMessage = "Error in normalize_data_in_staging: " + err.message;

        // Log the error
        try {
            snowflake.execute({
                sqlText: "INSERT INTO {{UPLOAD_DB}}.PUBLIC.ERROR_LOG_TABLE (LOG_MESSAGE, PROCEDURE_NAME, PLATFORM) VALUES (?, ?, ?)",
                binds: [errorMessage, 'normalize_data_in_staging', platformArg]
            });
        } catch (logErr) {
            console.error("Failed to log error: " + logErr.message);
        }

        throw new Error(errorMessage);
    }
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE {{UPLOAD_DB}}.PUBLIC.NORMALIZE_DATA_IN_STAGING(STRING, STRING) TO ROLE web_app;
