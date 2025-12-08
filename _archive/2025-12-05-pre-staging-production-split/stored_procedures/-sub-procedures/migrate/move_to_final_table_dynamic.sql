GRANT USAGE ON PROCEDURE UPLOAD_DB.PUBLIC.MOVE_DATA_TO_FINAL_TABLE_DYNAMIC(VARCHAR, VARCHAR, VARCHAR) TO ROLE web_app;


CREATE OR REPLACE PROCEDURE UPLOAD_DB.PUBLIC.MOVE_DATA_TO_FINAL_TABLE_DYNAMIC("PLATFORM" VARCHAR, "TYPE" VARCHAR, "FILENAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
let sql_command;
try {
    const platform = PLATFORM.toLowerCase();
    const type = TYPE.toLowerCase();
    const lowerFilename = FILENAME.toLowerCase();
    
    // Log procedure start
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
        binds: [`Starting procedure - Platform: ${PLATFORM}, Type: ${TYPE}, Filename: ${FILENAME}`, 'move_data_to_final_table_dynamic', PLATFORM]
    });
    
    // if type is viewership or viewership + revenue
    if (type.includes("viewership")) {
        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
            binds: [`Starting viewership INSERT`, 'move_data_to_final_table_dynamic', PLATFORM]
        });
        
        sql_command = `
            INSERT INTO staging_assets.public.episode_details_test_staging(viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, minutes, hours, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views)
            SELECT id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), sum(tot_mov), sum(tot_hov), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Viewership', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views
            FROM test_staging.public.${platform}_viewership 
            WHERE deal_parent is not null and processed is null and ref_id is not null and asset_series is not null and tot_mov is not null and tot_hov is not null AND LOWER(filename) = '${lowerFilename}'
            GROUP BY all;
        `;
        
        const stmt = snowflake.createStatement({sqlText: sql_command});
        stmt.execute();
        const rowsAffected = stmt.getNumRowsAffected();
        
        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
            binds: [`Viewership INSERT completed. Rows affected: ${rowsAffected}`, 'move_data_to_final_table_dynamic', PLATFORM]
        });
    }

    // if type is revenue or viewership + revenue
    if (type.includes("revenue")) {
        // Check if there are records that should match revenue criteria first
        const revenueCheckQuery = `
            SELECT COUNT(*) FROM test_staging.public.${platform}_viewership
            WHERE deal_parent is not null and processed is null and ref_id is not null and asset_series is not null and revenue is not null and revenue > 0 AND LOWER(filename) = '${lowerFilename}'
        `;
        const revenueCheckResult = snowflake.execute({sqlText: revenueCheckQuery});
        let revenueCount = 0;
        if (revenueCheckResult.next()) {
            revenueCount = revenueCheckResult.getColumnValue(1);
        }
        
        snowflake.execute({
            sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
            binds: [`Revenue records found: ${revenueCount}`, 'move_data_to_final_table_dynamic', PLATFORM]
        });
        
        if (revenueCount > 0) {
            sql_command = `
                insert into staging_assets.public.episode_details_test_staging(
                    viewership_id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sessions, year, quarter, platform, viewership_partner, domain, label, filename, phase, week, day, unique_viewers, platform_content_id, views,
                    register_name, payment_amount, revenue_amount, payment_date, payment_type, payment_title, payment_description, payment_department, payment_adjustment, payment_quarter, payment_year, payment_month, payment_support_category, payment_filename
                )
                select
                    id, ref_id, deal_parent, platform_content_name, platform_series, asset_title, asset_series, content_provider, month, year_month_day, channel, channel_id, territory, territory_id, sum(tot_sessions), year, quarter, '${PLATFORM}', partner, 'Distribution Partners', 'Revenue', filename, CAST(phase AS VARCHAR) as phase, week, day, sum(unique_viewers) as unique_viewers, platform_content_id, sum(views) as views,
                    CONCAT(partner, ' Revenue ', territory), revenue, revenue, year_month_day, '', '', '', '', 'False', quarter, year, month, 'Revenue', filename
                from test_staging.public.${platform}_viewership
                where deal_parent is not null and processed is null and ref_id is not null and asset_series is not null and revenue is not null and revenue > 0 AND LOWER(filename) = '${lowerFilename}'
                GROUP BY ALL
            `;
            
            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
                binds: [`Executing revenue INSERT for ${revenueCount} records`, 'move_data_to_final_table_dynamic', PLATFORM]
            });
            
            const revenueStmt = snowflake.createStatement({sqlText: sql_command});
            revenueStmt.execute();
            const revenueRowsAffected = revenueStmt.getNumRowsAffected();
            
            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
                binds: [`Revenue INSERT completed. Rows affected: ${revenueRowsAffected}`, 'move_data_to_final_table_dynamic', PLATFORM]
            });
        } else {
            snowflake.execute({
                sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
                binds: [`No revenue records found - skipping revenue INSERT`, 'move_data_to_final_table_dynamic', PLATFORM]
            });
        }
    }
    
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?)", 
        binds: [`Procedure completed successfully`, 'move_data_to_final_table_dynamic', PLATFORM]
    });

    return "Data moved successfully.";

} catch (error) {
    const errorMessage = "Error in sql query: " + sql_command + " error: " + error;
    console.error("Error executing SQL command:", error.message);
    snowflake.execute({
        sqlText: "INSERT INTO upload_db.public.error_log_table (log_time, log_message, procedure_name, platform, error_message) VALUES (CURRENT_TIMESTAMP(), ?, ?, ?, ?)", 
        binds: [`PROCEDURE FAILED: ${errorMessage}`, 'move_data_to_final_table_dynamic', PLATFORM, error.message]
    });
    return "Error executing SQL command: " + error.message;
}
$$;