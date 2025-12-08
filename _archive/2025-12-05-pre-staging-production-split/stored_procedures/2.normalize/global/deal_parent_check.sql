GRANT USAGE ON PROCEDURE upload_db.public.set_deal_parent(VARCHAR, VARCHAR) TO ROLE web_app;

CREATE OR REPLACE PROCEDURE upload_db.public.set_deal_parent(platform VARCHAR, columns_to_match VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    // Function to execute SQL and return results
    function executeSQL(sql) {
        var statement = snowflake.createStatement({sqlText: sql});
        var result = statement.execute();
        var resultArr = [];
        while (result.next()) {
            resultArr.push(result.getColumnValue(1));
        }
        return resultArr;
    }
    
    // Parse columns to match from JSON array
    var columnsArray = JSON.parse(COLUMNS_TO_MATCH);
    
    // Construct dynamic JOIN condition
    var joinCondition = columnsArray.map(function(column) {
        return `v.${column} = ad.${column}`;
    }).join(' AND ');

    // Construct columns string for SELECT
    var columnsString = columnsArray.join(', ');

    // Update deal_parent for matching records
    var updateSql = `
    UPDATE test_staging.public.${PLATFORM}_viewership v
    SET deal_parent = ad.deal_parent
    FROM dictionary.public.active_deals ad
    WHERE ${joinCondition}
    AND v.deal_parent IS NULL`;
    try {
        executeSQL(updateSql);
    } catch (err) {
        return "Failed to update deal_parent: " + err;
    }

    // Get non-matching records
    var nonMatchingSql = `
    SELECT DISTINCT ${columnsString}
    FROM test_staging.public.${PLATFORM}_viewership v
    WHERE NOT EXISTS (
        SELECT 1
        FROM dictionary.public.active_deals ad
        WHERE ${joinCondition}
    )
    AND v.deal_parent IS NULL`;
    var nonMatchingRecords = executeSQL(nonMatchingSql);

    // If there are non-matching records, send an email
    if (nonMatchingRecords.length > 0) {
        var emailBody = "The following combinations have no matching active deals:\n\n";
        emailBody += nonMatchingRecords.join('\n');
        
        var sendEmailSql = `
        CALL SYSTEM$SEND_EMAIL(
            'upload_db.public.snowflake_email_sender',
            'tayloryoung@mvmediasales.com',
            'Viewership Automation Alert: No Active Deals Found',
            '${emailBody.replace(/'/g, "''")}'
        )`;
        
        try {
            executeSQL(sendEmailSql);
        } catch (err) {
            return "Updated deal_parent, but failed to send email: " + err;
        }
    }
    return "Succeeded. Updated deal_parent and sent email for non-matching records (if any).";
$$;

-- Grant execute permission to the web_app role
GRANT USAGE ON PROCEDURE upload_db.public.set_deal_parent(VARCHAR, VARCHAR) TO ROLE web_app;