const { SnowFlakeMethods, handleConnectionError, nodemailer } = require('/opt/nodejs/connect.js');
// const { SnowFlakeMethods, handleConnectionError } = require('./snowflake_layer');
// const nodemailer = require('nodemailer');


const runQueryWithoutBind =  async (query, totalStatements, message) => {
    return new Promise(async (resolve, reject) => {
        const snowflakeMethods = new SnowFlakeMethods();

        try {
            await snowflakeMethods.connect();
            const conn = snowflakeMethods.conn; 

            conn.execute({
                sqlText: query,
                parameters: { MULTI_STATEMENT_COUNT: totalStatements ?? 1 },
                complete: function (err, stmt, rows) {
                    if (err) {
                        console.error("Error in running query", err);
                        console.error("query: " + query);
                        // Adjust the handleConnectionError call as needed based on its implementation
                        handleConnectionError(err, query, undefined, totalStatements ?? 1)
                            .then(({ error, rows }) => {
                                if (!error) {
                                    resolve({ message: message ?? "Query successfully executed", rows, stmt });
                                } else {
                                    reject({ err: error });
                                }
                            })
                            .catch(error => reject({ err: error }));
                    } else {
                        resolve({ message: message ?? "Query successfully executed", rows, stmt });
                    }
                },
            });
        } catch (connectionError) {
            console.error("Error establishing connection with Snowflake:", connectionError);
            reject({ err: connectionError });
        }
    });
};

async function verifyPhase(platform, fullyQualifiedDatabaseInstance, phase, file_record_count, filename, type, unmatchedRecordsTable = null) {
        let phaseCondition = phase === null ? "(phase is null or phase = '') " : `phase = '${phase}'`;

        let sqlTextViewership, sqlTextRevenue, sqlText;

        if (!fullyQualifiedDatabaseInstance) {
            console.error('fullyQualifiedDatabaseInstance is undefined');
            return false;
        }

        // if db instance is final table (e.g. assets.public.episode_details) table then check for revenue and viewership both based on type 
        //      if type includes revenue then revenue
        //      if type includes viewership then viewership 
        //      type can contain viewership and revenue both so using if condition only 
        // else
        //      check for non processed(processed is null) records in base viewership_table
        const uploadType = type?.toLowerCase()?.trim() ?? "";
        if (fullyQualifiedDatabaseInstance.toLowerCase().includes('episode')) {
            // phase can't exceed 2
            sqlTextViewership = uploadType.includes("viewership") ? 
                `SELECT COUNT(*) AS VIEWERSHIP_RECORDS_COUNT
                FROM ${fullyQualifiedDatabaseInstance}
                WHERE platform = '${platform}' AND processed IS NULL AND ${phaseCondition} AND filename = '${filename}' AND label = 'Viewership';`
                : null;

            sqlTextRevenue = uploadType.includes("revenue") ? 
                `SELECT COUNT(*) AS REVENUE_RECORDS_COUNT
                FROM ${fullyQualifiedDatabaseInstance}
                WHERE platform = '${platform}' AND processed IS NULL AND ${phaseCondition} AND filename = '${filename}' AND label = 'Revenue';` 
                : null;
        } else {
            // Previous approach - if asset matching has been done-> Also check for that required matched assets(asset_series, asset_title, ref_id, content_provider)
            // This approach - Don't wait for asset matching to be completed but just save the matched data into final table
            //      Only matched data should be sent into final table. This has been handled in the procedure moving data into final table
            sqlText = `SELECT COUNT(*) AS MATCHING_RECORDS_COUNT
            FROM ${fullyQualifiedDatabaseInstance}
            WHERE platform = '${platform}' AND processed IS NULL AND ${phaseCondition} AND filename = '${filename}';`;
        }

        console.log(`SQL (viewership): ${sqlTextViewership}`);
        console.log(`SQL (revenue): ${sqlTextRevenue}`);
        console.log(`SQL (original): ${sqlText}`);

        try {
            let actual_count_viewership = 0;
            let actual_count_revenue = 0;
            let unmatched_count = 0;

            if (sqlTextViewership || sqlTextRevenue) {
                console.log('Executing viewership and revenue count queries.');

                // Only execute viewership query if SQL text is not null
                if (sqlTextViewership) {
                    const resultViewership = await runQueryWithoutBind(sqlTextViewership);
                    if (resultViewership && resultViewership.rows && resultViewership.rows.length > 0) {
                        actual_count_viewership = resultViewership.rows[0].VIEWERSHIP_RECORDS_COUNT;
                        console.log(`Viewership count: ${actual_count_viewership}`);
                    } else {
                        console.log('No viewership records found or empty result set.');
                    }
                }

                // Only execute revenue query if SQL text is not null
                if (sqlTextRevenue) {
                    const resultRevenue = await runQueryWithoutBind(sqlTextRevenue);
                    if (resultRevenue && resultRevenue.rows && resultRevenue.rows.length > 0) {
                        actual_count_revenue = resultRevenue.rows[0].REVENUE_RECORDS_COUNT;
                        console.log(`Revenue count: ${actual_count_revenue}`);
                    } else {
                        console.log('No revenue records found or empty result set.');
                    }
                }

                if (phase === '2' && fullyQualifiedDatabaseInstance.toLowerCase().includes('assets')) {
                    const unmatchedRecordsQuery = `SELECT COUNT(*) AS UNMATCHED_RECORDS_COUNT
                    FROM ${unmatchedRecordsTable}
                    WHERE filename = '${filename}';`; // Updated to remove label condition

                    console.log(`Additional query for phase 2 and assets: ${unmatchedRecordsQuery}`);

                    try {
                        const unmatchedRecords = await runQueryWithoutBind(unmatchedRecordsQuery);
                        if (unmatchedRecords && unmatchedRecords.rows && unmatchedRecords.rows.length > 0) {
                            unmatched_count = unmatchedRecords.rows[0].UNMATCHED_RECORDS_COUNT;
                            console.log(`Unmatched records count: ${unmatched_count}`);
                        }
                    } catch (error) {
                        console.log(`Error executing additional query for phase 2 and assets:`, error);
                    }
                }
            } else {
                console.log('Executing original single query: SQL (original)');
                const result = await runQueryWithoutBind(sqlText);
                if (result && result.rows && result.rows.length > 0) {
                    actual_count_viewership = result.rows[0].MATCHING_RECORDS_COUNT;
                    console.log(`Matching records count: ${actual_count_viewership}`);
                } else {
                    console.log('No records found or empty result set.');
                }
            }

            // Calculate expected total count dynamically for viewership and revenue separately
            let expected_total_count_viewership = file_record_count;
            let expected_total_count_revenue = file_record_count;

            // Verify separately for viewership and revenue
            const actual_total_count_viewership = actual_count_viewership + unmatched_count;
            const actual_total_count_revenue = actual_count_revenue + unmatched_count;

            console.log(`Expected count (viewership): ${expected_total_count_viewership}, Actual count (viewership): ${actual_total_count_viewership}`);
            console.log(`Expected count (revenue): ${expected_total_count_revenue}, Actual count (revenue): ${actual_total_count_revenue}`);

            const isViewershipVerified = actual_total_count_viewership === expected_total_count_viewership;
            const isRevenueVerified = actual_total_count_revenue === expected_total_count_revenue;

            if (isViewershipVerified || isRevenueVerified) { // One of the verifications should pass
                if (isViewershipVerified) {
                    console.log(`Phase ${phase} verified: Expected count (viewership): ${expected_total_count_viewership}, actual count match (viewership): ${actual_total_count_viewership}`);
                }
                if (isRevenueVerified) {
                    console.log(`Phase ${phase} verified: Expected count (revenue): ${expected_total_count_revenue}, actual count match (revenue): ${actual_total_count_revenue}`);
                }
                return true;
            } else {
                console.log(`Phase verification failed for phase: ${phase}`);
                if (!isViewershipVerified) {
                    console.log(`Viewership count mismatch: Expected count: ${expected_total_count_viewership}, Actual count: ${actual_total_count_viewership}`);
                }
                if (!isRevenueVerified) {
                    console.log(`Revenue count mismatch: Expected count: ${expected_total_count_revenue}, Actual count: ${actual_total_count_revenue}`);
                }
            }
        } catch (error) {
            console.log(`Error executing query for phase ${phase}:`, error);
        }

    console.log(`Max retries reached for phase verification. Phase: ${phase}, Expected count: ${file_record_count * 2}.`);
    return false;
}

async function calculateViewershipMetrics(platform, filename, databaseName) {
    const storedProcedureStatement = `CALL ${databaseName}.public.calculate_viewership_metrics('${platform}', '${filename}');`;
    console.log("🚀 ~ calculateViewershipMetrics ~ storedProcedureStatement:", storedProcedureStatement);
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error calculating viewership metrics:', error);
        throw new Error(`Calculate viewership metrics failed: ${error.message || error}`);
    });
    console.log('✓ Viewership metrics calculated (TOT_HOV/TOT_MOV).');
}

async function setDateColumns(platform, filename, databaseName) {
    const storedProcedureStatement = `CALL ${databaseName}.public.set_date_columns_dynamic('${platform}', '${filename}');`;
    console.log("🚀 ~ setDateColumns ~ storedProcedureStatement:", storedProcedureStatement);
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error setting date columns:', error);
        throw new Error(`Set date columns failed: ${error.message || error}`);
    });
    console.log('✓ Date columns set (full_date, week, day, quarter, year, month).');
}

async function startDataProcessing(platform, databaseName, filename) {
    console.log("platform: ", platform);
    // Updated to call generic stored procedure instead of platform-specific one
    const storedProcedureStatement = `CALL ${databaseName}.public.move_viewership_to_staging('${platform}', '${filename}');`;
    console.log("DB call:", storedProcedureStatement);
    console.log("🚀 ~ startDataProcessing ~ storedProcedureStatement:", storedProcedureStatement)
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error calling start data processing stored procedure:', error);
        throw new Error(`Start data processing failed: ${error.message || error}`);
    });
}

async function normalizeData(platform, databaseName, filename) {
    // Updated to call generic stored procedure instead of platform-specific one
    const storedProcedureStatement = `CALL ${databaseName}.public.normalize_data_in_staging('${platform}', '${filename}');`;
    console.log("🚀 ~ normalizeData ~ storedProcedureStatement:", storedProcedureStatement)
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error calling normalize data processing stored procedure:', error);
        throw new Error(`Normalize data processing failed: ${error.message || error}`);
    });
    console.log('Normalizing data completed.');
}

async function calculateShares(platform) {
    const storedProcedureStatement = `CALL upload_db.public.calculate_shares_${platform}();`;
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error running share calculations stored procedure:', error);
        throw new Error(`Share calculations processing failed: ${error.message || error}`);
    });
    console.log('Share calculations completed.');
}

async function setContentReferences(platform, filename, databaseName) {
    // Phase 2: Set all normalized fields (deal_parent, partner, channel, territory, etc.)
    const setDealParent = `call ${databaseName}.public.set_deal_parent_generic('${platform}', '${filename}')`
    const setChannel = `call ${databaseName}.public.set_channel_generic('${platform}', '${filename}')`
    const setTerritory = `call ${databaseName}.public.set_territory_generic('${platform}', '${filename}')`
    const setDealParentNormalized = `call ${databaseName}.public.set_deal_parent_normalized_generic('${platform}', '${filename}')`
    const sendAlert = `call ${databaseName}.public.send_unmatched_deals_alert('${platform}', '${filename}')`

    // NEW: Extract series from titles using CONTAINS matching against dictionary
    // This helps platforms (like Youtube) where series name is embedded in the title
    const stagingDb = process.env.SNOWFLAKE_VIEWERSHIP_DATABASE;
    const viewershipTableFullyQualified = `${stagingDb}.public.platform_viewership`;
    const setInternalSeriesExtraction = `call ${databaseName}.public.SET_INTERNAL_SERIES_WITH_EXTRACTION('${viewershipTableFullyQualified}', 'filename', '${filename}')`

    // OLD: Fallback for platforms that have platform_series field populated
    const setInternalSeries = `call ${databaseName}.public.set_internal_series_generic('${platform}', '${filename}')`

    const dynamicAssetMatching = `call ${databaseName}.public.analyze_and_process_viewership_data_generic('${platform}', '${filename}');`;
    const setPhaseTwo = `call ${databaseName}.public.set_phase_generic('${platform}', '2', '${filename}')`

    console.log({setDealParent, setChannel, setTerritory, setDealParentNormalized, sendAlert, setInternalSeriesExtraction, setInternalSeries, dynamicAssetMatching, setPhaseTwo, platform});
    try {
        // Primary: Match against active_deals using RAW platform_* fields
        await runQueryWithoutBind(setDealParent)
        // Fallback: Pattern match channel for unmatched records
        await runQueryWithoutBind(setChannel)
        // Fallback: Normalize territory for unmatched records
        await runQueryWithoutBind(setTerritory)
        // Fallback: Match using normalized fields (partner, channel, territory)
        await runQueryWithoutBind(setDealParentNormalized)
        // Alert: Send email for any remaining unmatched records
        await runQueryWithoutBind(sendAlert)
        // Asset matching: Extract series from title (NEW - for platforms like Youtube)
        await runQueryWithoutBind(setInternalSeriesExtraction)
        // Asset matching: Set internal_series from platform_series field (OLD - fallback)
        await runQueryWithoutBind(setInternalSeries)
        // Asset matching: Match content using various strategies
        await runQueryWithoutBind(dynamicAssetMatching)
        // Update phase to 2
        await runQueryWithoutBind(setPhaseTwo)
    } catch (error) {
        console.error(error);
        throw error;
    }
    console.log('Setting content references completed.');
}

async function markDataAsProcessed(fullyQualifiedDatabaseInstance, filename) {
    let sqlText;
    if (fullyQualifiedDatabaseInstance.toLowerCase().includes("staging")) {
        sqlText = `UPDATE ${fullyQualifiedDatabaseInstance} SET processed = TRUE WHERE filename = '${filename}' AND REF_ID is not null AND ASSET_SERIES is not null AND CONTENT_PROVIDER is not null;`;
    } else {
        sqlText = `UPDATE ${fullyQualifiedDatabaseInstance} SET processed = TRUE WHERE filename = '${filename}';`;
    }
    console.log("🚀 ~ markDataAsProcessed ~ sqlText:", sqlText)
    
    await runQueryWithoutBind(sqlText).catch(error => {
        console.error('Error marking data as processed:', error);
        throw new Error('Error in marking data as processed');
    });
    console.log('Data marked as processed.');
}

async function moveToFinalTable(platform, databaseName, type, filename) {
    const storedProcedureStatement = `CALL ${databaseName}.public.handle_final_insert_dynamic_generic('${platform}', '${type}', '${filename}');`;
    console.log("🚀 ~ normalizeData ~ storedProcedureStatement:", storedProcedureStatement)
    console.log("🚀 ~ moveToFinalTable ~ storedProcedureStatement:", storedProcedureStatement)
    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error moving data to final table:', error);
        throw new Error(`Error moving data to final table: ${error.message || error}`);
    });
    console.log('Data moved to final table.');
};



async function sendEmail(recipientEmailList, platform, emailSubjectBase, body, csvFile = null, fileName = null) {
    try {
        //Email subject
        const emailSubject = `${platform} - ${emailSubjectBase}`;

        // Create a transporter using SMTP transport
        const transporter = nodemailer.createTransport({
            service: 'gmail',
            auth: {
                  user: process.env.MAILER_EMAIL,
                  pass: process.env.MAILER_PASSWORD,
            },
        });
        // Email content
        const mailOptions = {
            from: `Marathon Ventures<${process.env.MAILER_EMAIL}>`,
            to: recipientEmailList,
            cc: 'data@nosey.com',
            subject: emailSubject,
            text: body,
        };
        if (csvFile) {
            mailOptions["attachments"] = {
                filename: fileName ?? "data.csv",
                content: csvFile
            }
        }

        // Send the email
        const data = await transporter.sendMail(mailOptions)
        console.log("Email sent");

    } catch (err) {
        console.error("error in sending the email", { err })
        throw new Error("error sending mail")
    }
}

/**
 * Move data from upload_db to viewership_db for Streamlit uploads
 * Dynamically reads column schema to avoid hardcoding
 */
async function moveDataToStaging(uploadDatabaseName, viewershipDatabaseFullyQualified, platform, filename) {
    console.log(`Moving data from ${uploadDatabaseName} to staging for Streamlit upload...`);

    // Step 1: Get all column names from source table dynamically
    const getColumnsSQL = `
        SELECT COLUMN_NAME
        FROM ${uploadDatabaseName}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'PUBLIC'
          AND TABLE_NAME = 'PLATFORM_VIEWERSHIP'
          AND TABLE_CATALOG = '${uploadDatabaseName.toUpperCase()}'
        ORDER BY ORDINAL_POSITION;
    `;

    console.log('Fetching column schema from source table...');
    const columnsResult = await runQueryWithoutBind(getColumnsSQL);

    if (!columnsResult || !columnsResult.rows || columnsResult.rows.length === 0) {
        throw new Error('Failed to fetch column schema from platform_viewership table');
    }

    // Extract column names, excluding LOAD_TIMESTAMP (will be auto-generated)
    const columns = columnsResult.rows
        .map(row => row.COLUMN_NAME)
        .filter(col => col.toUpperCase() !== 'LOAD_TIMESTAMP');

    console.log(`Found ${columns.length} columns to copy`);

    // Step 2: Build INSERT statement - copy data as-is
    const columnList = columns.join(', ');

    const moveDataSQL = `
        INSERT INTO ${viewershipDatabaseFullyQualified} (${columnList})
        SELECT ${columnList}
        FROM ${uploadDatabaseName}.public.platform_viewership
        WHERE platform = '${platform}'
          AND filename = '${filename}'
          AND processed IS NULL
          AND (phase IS NULL OR phase = '');
    `;

    console.log("Streamlit - Move data SQL:", moveDataSQL);

    await runQueryWithoutBind(moveDataSQL).catch(error => {
        console.error('Error moving data to staging:', error);
        throw new Error(`Failed to move data to staging: ${error.message || error}`);
    });

    console.log(`✓ Data copied to staging`);

    // Step 3: Set phase to '0' using stored procedure
    const setPhaseSQL = `CALL ${uploadDatabaseName}.public.set_phase_generic('${platform}', 0, '${filename}')`;
    console.log("Setting phase to 0:", setPhaseSQL);

    await runQueryWithoutBind(setPhaseSQL).catch(error => {
        console.error('Error setting phase:', error);
        throw new Error(`Failed to set phase: ${error.message || error}`);
    });

    console.log(`✓ Phase set to 0 successfully`);
}

/**
 * Calculate missing viewership metrics (TOT_HOV from TOT_MOV or vice versa)
 * Runs stored procedure that updates records in test_staging
 */
async function calculateViewershipMetrics(platform, filename, databaseName) {
    const storedProcedureStatement = `CALL ${databaseName}.public.calculate_viewership_metrics('${platform}', '${filename}');`;
    console.log("🚀 ~ calculateViewershipMetrics ~ storedProcedureStatement:", storedProcedureStatement);

    await runQueryWithoutBind(storedProcedureStatement).catch(error => {
        console.error('Error calculating viewership metrics:', error);
        throw new Error(`Calculate viewership metrics failed: ${error.message || error}`);
    });

    console.log('✓ Viewership metrics calculated (TOT_HOV/TOT_MOV).');
}


module.exports.verifyPhase = verifyPhase;
module.exports.runQueryWithoutBind = runQueryWithoutBind;
module.exports.normalizeData = normalizeData;
module.exports.startDataProcessing = startDataProcessing;
module.exports.calculateShares = calculateShares;
module.exports.setContentReferences = setContentReferences;
module.exports.markDataAsProcessed = markDataAsProcessed;
module.exports.moveToFinalTable = moveToFinalTable;
module.exports.sendEmail = sendEmail;
module.exports.moveDataToStaging = moveDataToStaging;
module.exports.calculateViewershipMetrics = calculateViewershipMetrics;
module.exports.setDateColumns = setDateColumns;