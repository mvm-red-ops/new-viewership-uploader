CREATE OR REPLACE PROCEDURE upload_db.public.set_hov_mov_dynamic(platform VARCHAR)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    STRICT
    EXECUTE AS OWNER
AS
$$
var sql_commands = [
    // Update tot_mov based on tot_hov if tot_mov is null
    `UPDATE test_staging.public.${PLATFORM}_viewership SET tot_mov = tot_hov * 60 WHERE tot_hov IS NOT NULL AND tot_mov IS NULL AND processed IS NULL`,

    // Update tot_hov based on tot_mov if tot_hov is null
    `UPDATE test_staging.public.${PLATFORM}_viewership SET tot_hov = tot_mov / 60 WHERE tot_mov IS NOT NULL AND tot_hov IS NULL AND processed IS NULL`
];

try {
    for(var i = 0; i < sql_commands.length; i++) {
        snowflake.execute({sqlText: sql_commands[i]});
    }
    return "Succeeded.";   // Return a success/error indicator.
}
catch (err) {
    return "Failed: " + err;   // Return a success/error indicator.
}
$$;
