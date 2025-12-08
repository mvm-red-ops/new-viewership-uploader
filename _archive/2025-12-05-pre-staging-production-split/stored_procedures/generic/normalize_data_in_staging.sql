-- Generic Normalize Data in Staging Procedure
-- Replaces platform-specific normalize_data_in_staging_{platform} procedures
-- Orchestrates data normalization phase (Phase 1) by calling platform-specific helpers

CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING(
    platform STRING,
    filename STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Helper function to call stored procedures and handle errors
    function callProcedure(procName, args) {
        try {
            var sql_command = 'CALL ' + procName;
            if (args && args.length > 0) {
                sql_command += `(${args.map(arg => `'${arg}'`).join(',')})`;
            } else {
                sql_command += '()';
            }
            snowflake.execute({sqlText: sql_command});
            return { success: true, message: "Succeeded" };
        } catch (err) {
            throw new Error("Procedure " + procName + " failed: " + err.message);
        }
    }

    const upperPlatform = PLATFORM.toUpperCase();
    const lowerFilename = FILENAME.toLowerCase();

    try {
        // Step 1: Look up platform configuration
        var configQuery = `
            SELECT
                has_custom_territory_mapping,
                has_custom_channel_mapping,
                has_custom_date_handling,
                territory_procedure,
                channel_procedure,
                deal_parent_procedure,
                date_handling_procedure,
                additional_normalizers,
                deal_parent_columns
            FROM dictionary.public.platform_config
            WHERE UPPER(platform_name) = '${upperPlatform}'
              AND active = TRUE
        `;

        var configStmt = snowflake.createStatement({sqlText: configQuery});
        var configResult = configStmt.execute();

        if (!configResult.next()) {
            throw new Error(`Platform '${PLATFORM}' not found in platform_config or is inactive`);
        }

        var hasCustomTerritory = configResult.getColumnValue('HAS_CUSTOM_TERRITORY_MAPPING');
        var hasCustomChannel = configResult.getColumnValue('HAS_CUSTOM_CHANNEL_MAPPING');
        var hasCustomDate = configResult.getColumnValue('HAS_CUSTOM_DATE_HANDLING');
        var territoryProc = configResult.getColumnValue('TERRITORY_PROCEDURE');
        var channelProc = configResult.getColumnValue('CHANNEL_PROCEDURE');
        var dealParentProc = configResult.getColumnValue('DEAL_PARENT_PROCEDURE');
        var dateProc = configResult.getColumnValue('DATE_HANDLING_PROCEDURE');
        var additionalNormalizers = configResult.getColumnValue('ADDITIONAL_NORMALIZERS');
        var dealParentColumns = configResult.getColumnValue('DEAL_PARENT_COLUMNS');

        var procedures = [];

        // Step 2: Add territory mapping procedure
        if (hasCustomTerritory && territoryProc) {
            procedures.push({
                name: `upload_db.public.${territoryProc}`,
                args: [FILENAME]
            });
        }

        // Step 3: Add channel mapping procedure
        if (hasCustomChannel && channelProc) {
            procedures.push({
                name: `upload_db.public.${channelProc}`,
                args: [FILENAME]
            });
        }

        // Step 4: Add deal parent procedure
        if (dealParentProc) {
            if (dealParentColumns) {
                // Dynamic deal parent with column configuration
                procedures.push({
                    name: `upload_db.public.${dealParentProc}`,
                    args: [PLATFORM, dealParentColumns, FILENAME]
                });
            } else {
                // Platform-specific deal parent procedure
                procedures.push({
                    name: `upload_db.public.${dealParentProc}`,
                    args: [FILENAME]
                });
            }
        }

        // Step 5: Add date handling procedure
        if (hasCustomDate && dateProc) {
            procedures.push({
                name: `upload_db.public.${dateProc}`,
                args: [PLATFORM, FILENAME]
            });
        }

        // Step 6: Add additional normalizers (from JSON array)
        if (additionalNormalizers) {
            try {
                var additionalProcs = JSON.parse(additionalNormalizers);
                for (var i = 0; i < additionalProcs.length; i++) {
                    var procName = additionalProcs[i];
                    procedures.push({
                        name: `upload_db.public.${procName}`,
                        args: [PLATFORM, FILENAME]
                    });
                }
            } catch (parseErr) {
                console.error("Error parsing additional_normalizers JSON: " + parseErr.message);
            }
        }

        // Step 7: Always set phase to 1 at the end
        procedures.push({
            name: "upload_db.public.set_phase_generic",
            args: [PLATFORM, "1", FILENAME]
        });

        // Step 8: Execute all procedures in sequence
        for (var j = 0; j < procedures.length; j++) {
            var proc = procedures[j];
            console.log(`Calling: ${proc.name} with args: ${proc.args}`);
            var result = callProcedure(proc.name, proc.args);
            if (!result.success) {
                return result.message;
            }
        }

        return `All normalization procedures executed successfully for ${PLATFORM} - ${FILENAME}`;

    } catch (err) {
        const errorMessage = "Error in normalize_data_in_staging: " + err.message;

        // Log the error
        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_message, procedure_name, platform) VALUES (?, ?, ?)",
            binds: [errorMessage, 'normalize_data_in_staging', PLATFORM]
        });

        return errorMessage;
    }
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.NORMALIZE_DATA_IN_STAGING(STRING, STRING) TO ROLE web_app;
