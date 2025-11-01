-- ==============================================================================
-- Calculate Missing Viewership Metrics
-- ==============================================================================
-- Calculates TOT_HOV from TOT_MOV or TOT_MOV from TOT_HOV when only one exists
-- Should only run for "Viewership" type data (not Revenue)
-- ==============================================================================

CREATE OR REPLACE PROCEDURE upload_db.public.calculate_viewership_metrics(
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
        UPDATE test_staging.public.platform_viewership
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
        UPDATE test_staging.public.platform_viewership
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
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)",
        binds: [`Calculated missing viewership metrics. Total records updated: ${updatedRecords}`, 'calculate_viewership_metrics', PLATFORM]
    });

    return `Successfully calculated missing metrics for ${updatedRecords} records`;

} catch (error) {
    const errorMessage = "Error in calculate_viewership_metrics: " + error.message;
    console.error(errorMessage);

    // Log error
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)",
        binds: [errorMessage, 'calculate_viewership_metrics', PLATFORM, error.message]
    });

    return errorMessage;
}
$$;

GRANT USAGE ON PROCEDURE upload_db.public.calculate_viewership_metrics(VARCHAR, VARCHAR) TO ROLE web_app;
