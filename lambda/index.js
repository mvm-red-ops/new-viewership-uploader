// require('dotenv').config()
const { SnowFlakeMethods } = require('/opt/nodejs/connect.js');
// const { SnowFlakeMethods } = require('./snowflake_layer.js');

const { startDataProcessing, normalizeData, verifyPhase, setContentReferences, markDataAsProcessed, moveToFinalTable, sendEmail, runQueryWithoutBind, moveDataToStaging, calculateViewershipMetrics, setDateColumns} = require('./snowflake-helpers.js');

// type = Revenue | Viewership_Revenue | Viewership | Payment
// platform = Pluto | Wurl
// if type = Viewership_Revenue and platform = Pluto, then territory should be US

const handler = async (event) => {
    try {
        const snowflakeMethods = new SnowFlakeMethods();
        await snowflakeMethods.connect();
        // Extract all parameters including new selectors for generic table queries
        const { record_count, tot_hov, platform, domain, filename, userEmail, type, territory, channel, year, quarter, month, jobType } = event;

        // Log the received parameters for debugging
        console.log('Lambda invoked with parameters:', {
            platform, filename, territory, channel, year, quarter, month,
            record_count, type, domain, jobType
        });

        const uploadDatabaseName = process.env.SNOWFLAKE_UPLOADER_DATABASE;
        const vewershipDatabaseName = process.env.SNOWFLAKE_VIEWERSHIP_DATABASE;
        // Updated to use generic platform_viewership table instead of platform-specific tables
        const uploadDatabaseFullyQualified = `${uploadDatabaseName}.PUBLIC.platform_viewership`;
        const viewershipDatabaseFullyQualified = `${vewershipDatabaseName}.PUBLIC.platform_viewership`;
        const metadataDatabaseName = process.env.METADATA_DATABASE;
        const reprocessingLogTable = process.env.METADATA_LOG_TABLE;
        const reprocessingLogFullyQualified = `${metadataDatabaseName}.PUBLIC.${reprocessingLogTable}`;
        const episodeDetailsDatabaseName = process.env.EPISODE_DETAILS_DATABASE;
        const episodeDetailsTableName = process.env.EPISODE_DETAILS_TABLE;
        const episodeDetailsDatabaseFullyQualified = `${episodeDetailsDatabaseName}.PUBLIC.${episodeDetailsTableName}`;

        // ============================================================
        // ROUTE BASED ON JOB TYPE
        // ============================================================
        if (jobType === "Streamlit") {
            // NEW PATH: Streamlit uploads (data already uploaded & normalized)
            console.log('Using Streamlit processing path (skip Phase 0 & 1)');
            return await processStreamlitUpload(
                snowflakeMethods,
                platform, uploadDatabaseName, filename, record_count, type, userEmail, domain, tot_hov,
                viewershipDatabaseFullyQualified, episodeDetailsDatabaseFullyQualified, reprocessingLogFullyQualified
            );
        }

        // LEGACY PATH: S3 uploads (full processing pipeline)
        console.log('Using legacy S3 processing path (all phases)');

        // Call the verifyPhase function
        const verifyPhaseResult = await verifyPhase(platform, uploadDatabaseFullyQualified, null, record_count, filename, type);
        console.log("🚀 ~ handler ~ verifyPhaseResult-null:", verifyPhaseResult)

        if (!verifyPhaseResult.verified) {
            console.error('Initial verification failed:', verifyPhaseResult.reason);
            await sendEmail(userEmail, platform, "Processing Error", `Initial verification failed for ${filename}: ${verifyPhaseResult.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: verifyPhaseResult.reason }) };
        }

        // Start data processing by moving data to staging
        await startDataProcessing(platform, uploadDatabaseName, filename);

        // Verify Phase 0
        let phaseVerified = await verifyPhase(platform, viewershipDatabaseFullyQualified, '0', record_count, filename, type);
        if (!phaseVerified.verified) {
            console.error('Phase 0 failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error", `Phase 0 (move to staging) failed for ${filename}: ${phaseVerified.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Mark data as processed in upload db as we won't use this data in another step
        await markDataAsProcessed(uploadDatabaseFullyQualified, filename);

        // Normalize data
        await normalizeData(platform, uploadDatabaseName, filename);

        // Verify Phase 1
        phaseVerified = await verifyPhase(platform, viewershipDatabaseFullyQualified, '1', record_count, filename, type);
        if (!phaseVerified.verified) {
            console.error('Phase 1 failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error", `Phase 1 (normalization) failed for ${filename}: ${phaseVerified.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Set content references
        await setContentReferences(platform, filename, uploadDatabaseName);

        // Verify Phase 2
        phaseVerified = await verifyPhase(platform, viewershipDatabaseFullyQualified, '2', record_count, filename, type);
        if (!phaseVerified.verified) {
            console.error('Phase 2 failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error",
                `Phase 2 (content references) failed for ${filename}: ${phaseVerified.reason}\n\nCheck records in ${viewershipDatabaseFullyQualified} and update ref_id, asset_title, asset_series as needed.`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Move to final table
        await moveToFinalTable(platform, uploadDatabaseName, type, filename);

        // Verify Final Phase
        phaseVerified = await verifyPhase(platform, episodeDetailsDatabaseFullyQualified, '2', record_count, filename, type, reprocessingLogFullyQualified);
        if (!phaseVerified.verified) {
            console.error('Final phase failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error", `Final phase (move to final table) failed for ${filename}: ${phaseVerified.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Mark data as processed in viewership db
        await markDataAsProcessed(viewershipDatabaseFullyQualified, filename);

        //send confirmation email
        const confirmationEmailText = `Your data processing is complete for 
            Platform: ${platform}, 
            Domain: ${domain}, 
            Type: ${type},
            Filename: ${filename},
            Total Records: ${record_count},
            Total Hours of Viewership: ${tot_hov}
        `
        await sendEmail(userEmail, platform, "Processing Complete", confirmationEmailText);

        // Return the results
        return {
            statusCode: 200,
            body: JSON.stringify({ verifyPhaseResult })
        };
    } catch (error) {
        console.error('Error:', error);
        return {
            statusCode: 200, // 500
            body: JSON.stringify({ error: error.message })
        };
    }
};

/**
 * Process Streamlit uploads (data already uploaded & normalized in Python)
 * This path skips Phase 0 (move to staging) and Phase 1 (normalization)
 * and goes directly to Phase 2 (content references) and Phase 3 (final table)
 */
async function processStreamlitUpload(
    snowflakeMethods,
    platform, uploadDatabaseName, filename, record_count, type, userEmail, domain, tot_hov,
    viewershipDatabaseFullyQualified, episodeDetailsDatabaseFullyQualified, reprocessingLogFullyQualified
) {
    try {
        console.log('Starting Streamlit upload post-processing...');

        // Verify data exists in upload_db.public.platform_viewership
        const uploadDatabaseFullyQualified = `${uploadDatabaseName}.PUBLIC.platform_viewership`;
        const initialVerifyResult = await verifyPhase(platform, uploadDatabaseFullyQualified, null, record_count, filename, type);
        console.log("Streamlit - Initial verification:", initialVerifyResult);

        if (!initialVerifyResult.verified) {
            console.error('Initial verification failed:', initialVerifyResult.reason);
            await sendEmail(userEmail, platform, "Processing Error",
                `Initial verification failed for ${filename}: ${initialVerifyResult.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: initialVerifyResult.reason }) };
        }

        // Move data to staging for processing (data already normalized, just need to move it)
        // For Streamlit, use simple SQL instead of stored procedures
        await moveDataToStaging(uploadDatabaseName, viewershipDatabaseFullyQualified, platform, filename);

        // Verify data moved to staging
        let phaseVerified = await verifyPhase(platform, viewershipDatabaseFullyQualified, '0', record_count, filename, type);
        console.log("Streamlit - Phase 0 verified:", phaseVerified);

        if (!phaseVerified.verified) {
            console.error('Phase 0 failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error",
                `Phase 0 (move to staging) failed for ${filename}: ${phaseVerified.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Mark data as processed in upload db
        await markDataAsProcessed(uploadDatabaseFullyQualified, filename);

        // Calculate missing viewership metrics (TOT_HOV from TOT_MOV or vice versa)
        // For Viewership and Viewership_Revenue types (not pure Revenue)
        if (type && type.toLowerCase().includes('viewership')) {
            console.log('Calculating missing viewership metrics (TOT_HOV/TOT_MOV)...');
            await calculateViewershipMetrics(platform, filename, uploadDatabaseName);
        }

        // Set date columns (full_date, week, day, quarter, year, month, year_month_day)
        console.log('Setting date columns...');
        await setDateColumns(platform, filename, uploadDatabaseName);

        // PHASE 2: Set content references (asset matching)
        console.log('Starting Phase 2: Content references (asset matching)...');
        await setContentReferences(platform, filename, uploadDatabaseName);

        // Verify Phase 2
        phaseVerified = await verifyPhase(platform, viewershipDatabaseFullyQualified, '2', record_count, filename, type);
        console.log("Streamlit - Phase 2 verified:", phaseVerified);

        if (!phaseVerified.verified) {
            console.error('Phase 2 failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error",
                `Phase 2 (content references) failed for ${filename}: ${phaseVerified.reason}\n\nCheck records in ${viewershipDatabaseFullyQualified} and update ref_id, asset_title, asset_series as needed.`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // PHASE 3: Move to final table
        console.log('Starting Phase 3: Move to final table...');
        await moveToFinalTable(platform, uploadDatabaseName, type, filename);

        // Verify final phase
        phaseVerified = await verifyPhase(platform, episodeDetailsDatabaseFullyQualified, '2', record_count, filename, type, reprocessingLogFullyQualified);
        console.log("Streamlit - Final phase verified:", phaseVerified);

        if (!phaseVerified.verified) {
            console.error('Final phase failed:', phaseVerified.reason);
            await sendEmail(userEmail, platform, "Processing Error", `Final phase (move to final table) failed for ${filename}: ${phaseVerified.reason}`);
            return { statusCode: 200, body: JSON.stringify({ message: phaseVerified.reason }) };
        }

        // Mark data as processed in viewership db
        await markDataAsProcessed(viewershipDatabaseFullyQualified, filename);

        // Send confirmation email
        const confirmationEmailText = `Your data processing is complete for
            Platform: ${platform},
            Domain: ${domain},
            Type: ${type},
            Filename: ${filename},
            Total Records: ${record_count},
            Total Hours of Viewership: ${tot_hov}

            Processing Path: Streamlit (with transformations)
        `;
        await sendEmail(userEmail, platform, "Processing Complete", confirmationEmailText);

        console.log('Streamlit upload post-processing completed successfully');

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Streamlit upload processed successfully',
                verifyPhaseResult: initialVerifyResult
            })
        };

    } catch (error) {
        console.error('Error in Streamlit processing:', error);
        await sendEmail(userEmail, platform, "Processing Error",
            `Error during post-processing: ${error.message}`);
        return {
            statusCode: 200,
            body: JSON.stringify({ error: error.message })
        };
    }
}

module.exports.handler = handler;

