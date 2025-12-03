-- Generic Move Viewership to Staging Procedure
-- Replaces platform-specific move_viewership_to_staging_{platform} procedures
-- Handles initial data processing phase (Phase 0)

CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Helper function to execute a stored procedure and handle errors
        function executeStoredProcedure(procName, args) {
            try {
                var sqlCommand = `CALL ` + procName;
                if (args && args.length > 0) {
                    sqlCommand += `(${args.map(arg => `'${arg}'`).join(', ')})`;
                } else {
                    sqlCommand += '()';
                }
                snowflake.execute({sqlText: sqlCommand});
                return { success: true, message: procName + " executed successfully." };
            } catch (err) {
                console.error("Error executing " + procName + ": " + err.message);
                return {
                    success: false,
                    message: "Error executing " + procName + ": " + err.message + " | SQL: " + sqlCommand
                };
            }
        }

        const upperPlatform = PLATFORM.toUpperCase();
        const lowerFilename = FILENAME.toLowerCase();

        // Step 1: Look up platform configuration
        var configQuery = `
            SELECT
                has_custom_sanitization,
                sanitization_procedure
            FROM dictionary.public.platform_config
            WHERE UPPER(platform_name) = '${upperPlatform}'
              AND active = TRUE
        `;

        var configStmt = snowflake.createStatement({sqlText: configQuery});
        var configResult = configStmt.execute();

        if (!configResult.next()) {
            throw new Error(`Platform '${PLATFORM}' not found in platform_config or is inactive`);
        }

        var hasCustomSanitization = configResult.getColumnValue('HAS_CUSTOM_SANITIZATION');
        var sanitizationProc = configResult.getColumnValue('SANITIZATION_PROCEDURE');

        // Step 2: Execute sanitization if needed
        if (hasCustomSanitization && sanitizationProc) {
            console.log(`Calling custom sanitization: ${sanitizationProc}`);
            var sanitizeResult = executeStoredProcedure(
                `UPLOAD_DB_PROD.public.${sanitizationProc}`,
                [FILENAME]
            );
            if (!sanitizeResult.success) {
                throw new Error(sanitizeResult.message);
            }
        } else {
            console.log("No custom sanitization required, using generic sanitization");
            var genericSanitizeResult = executeStoredProcedure(
                'UPLOAD_DB_PROD.public.generic_sanitization',
                [PLATFORM, FILENAME]
            );
            if (!genericSanitizeResult.success) {
                throw new Error(genericSanitizeResult.message);
            }
        }

        // Step 3: Move data from upload_db to NOSEY_PROD
        var moveResult = executeStoredProcedure(
            'UPLOAD_DB_PROD.public.move_sanitized_data_to_staging_generic',
            [PLATFORM, FILENAME]
        );
        if (!moveResult.success) {
            throw new Error(moveResult.message);
        }

        // Step 4: Set phase to 0 (initial load complete)
        var phaseResult = executeStoredProcedure(
            'UPLOAD_DB_PROD.public.set_phase_generic',
            [PLATFORM, '0', FILENAME]
        );
        if (!phaseResult.success) {
            throw new Error(phaseResult.message);
        }

        console.log(`Successfully moved ${PLATFORM} viewership data to staging for file: ${FILENAME}`);
        return "All procedures executed successfully.";

    } catch (error) {
        const errorMessage = "Error in move_viewership_to_staging: " + error.message;

        // Log the error
        snowflake.execute({
            sqlText: "INSERT INTO UPLOAD_DB_PROD.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)",
            binds: [errorMessage, 'move_viewership_to_staging', PLATFORM]
        });

        return errorMessage;
    }
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB_PROD.PUBLIC.MOVE_VIEWERSHIP_TO_STAGING(STRING, STRING) TO ROLE web_app;
