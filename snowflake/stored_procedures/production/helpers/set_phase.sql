CREATE OR REPLACE PROCEDURE UPLOAD_DB_PROD.public.set_phase_dynamic(platform VARCHAR, phase_number FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Declare JavaScript variables and assign them the values of the SQL variables
    var jsPlatform = PLATFORM;
    var jsPhaseNumber = PHASE_NUMBER;

    // Construct the SQL command using the JavaScript variables
    var sql_command = `UPDATE NOSEY_PROD.public.${jsPlatform}_viewership SET phase = ? WHERE processed IS NULL`;
    var statement = snowflake.createStatement({
        sqlText: sql_command,
        binds: [jsPhaseNumber]
    });
    statement.execute();
    return "Succeeded.";
}
catch (err) {
    return "Failed: " + err.message; // Use err.message to get the error message
}
$$;
