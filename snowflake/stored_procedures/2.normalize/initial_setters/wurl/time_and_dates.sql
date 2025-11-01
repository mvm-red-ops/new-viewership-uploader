
CREATE OR REPLACE PROCEDURE upload_db.public.set_date_columns_wurl()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Function to execute a given SQL command and return a status message
    function executeSqlCommand(sql_command) {
        try {
            snowflake.execute({sqlText: sql_command});
            return "Succeeded";
        } catch (err) {
            return "Failed: " + err;
        }
    }
    
    var updateCommands = [
        // Update statement for getting the full date
        "UPDATE test_staging.public.wurl_viewership SET full_date = upload_db.public.get_full_date_from_wurl(date) WHERE processed is null",
        
        // Update statement for setting the week start date
        "UPDATE test_staging.public.wurl_viewership SET week_start = upload_db.public.get_week_start(get_full_date_from_wurl(date)) WHERE processed is null",
        
        // Update statement for setting the quarter
        "UPDATE test_staging.public.wurl_viewership SET quarter = upload_db.public.get_quarter_from_mm_dd_yyyy(get_full_date_from_wurl(date)) WHERE processed is null",
        
        // Update statement for setting the year
        "UPDATE test_staging.public.wurl_viewership SET year = upload_db.public.get_year_from_mm_dd_yyyy(get_full_date_from_wurl(date)) WHERE processed is null",
        
        // Update statement for setting the month
        "UPDATE test_staging.public.wurl_viewership SET month = upload_db.public.get_month_from_mm_dd_yyyy(get_full_date_from_wurl(date)) WHERE processed is null",
                
        // Update statement for setting the first of the month
        "UPDATE test_staging.public.wurl_viewership SET year_month_day = upload_db.public.get_first_of_month_from_mm_dd_yyyy(get_full_date_from_wurl(date)) WHERE processed is null",
        
        // Update statement for setting the day
        "UPDATE test_staging.public.wurl_viewership SET day = upload_db.public.get_day_from_mm_dd_yyyy(get_full_date_from_wurl(date)) WHERE processed is null"
    ];
    
    var resultMessage = "";
    for (var i = 0; i < updateCommands.length; i++) {
        resultMessage = executeSqlCommand(updateCommands[i]);
        if (resultMessage !== "Succeeded") {
            return "Error executing update command: " + resultMessage;
        }
    }
    
    return "All updates executed successfully.";
$$;

