# Service: Lambda Processing Orchestrator

## Purpose
AWS Lambda function that orchestrates the 3-phase data processing pipeline, handles retries, verification, and sends email notifications.

## Location
- **Source:** `lambda/index.js`
- **Function Name:**
  - Staging: `register-start-viewership-data-processing-staging`
  - Production: `register-start-viewership-data-processing`
- **Runtime:** Node.js
- **Timeout:** 15 minutes
- **Memory:** 512 MB (configurable)

## Invocation

### From Streamlit
```python
lambda_client.invoke(
    FunctionName='register-start-viewership-data-processing',
    InvocationType='Event',  # Async
    Payload=json.dumps({
        'platform': 'Philo',
        'filename': '20251106_103826.csv',
        'dataType': 'Viewership',  # or 'Revenue'
        'invokedBy': 'streamlit'
    })
)
```

### Manual Test
```javascript
{
  "platform": "Philo",
  "filename": "20251106_103826.csv",
  "dataType": "Viewership",
  "invokedBy": "manual-test"
}
```

## Three-Phase Processing

### Phase 1: Set Deal Parent & Series
```javascript
CALL set_deal_parent_generic('Philo', 'Viewership', '20251106_103826.csv');
CALL set_internal_series_generic('Philo', '20251106_103826.csv');
```

**Purpose:**
- Normalize partner/territory/channel from active_deals table
- Match platform series names to internal series dictionary

**Success Criteria:**
- Procedure completes without error
- Records have `deal_parent` set

**Verification:** Query count of records with `deal_parent IS NOT NULL`

### Phase 2: Content Matching & Metrics
```javascript
// 2a: Match content to metadata
CALL analyze_and_process_viewership_data_generic('Philo', '20251106_103826.csv');

// 2b: Set fallback territory/channel (for unmatched deals)
CALL set_territory_generic('Philo', '20251106_103826.csv');
CALL set_channel_generic('Philo', '20251106_103826.csv');

// 2c: Calculate metrics
CALL calculate_viewership_metrics('Philo', '20251106_103826.csv');

// 2d: Set date columns
CALL set_date_columns_dynamic('Philo', '20251106_103826.csv');

// 2e: Validate before final load
CALL validate_viewership_for_insert('Philo', '20251106_103826.csv');
```

**Success Criteria:**
- Content matching completes
- All records have required fields set
- Validation returns `{valid: true}`

**Verification:**
Count records in `platform_viewership` where:
- `processed IS NULL`
- `phase = '2'`
- `filename = '{filename}'`

Expected count = records in upload

### Phase 3: Move to Final Table
```javascript
CALL handle_final_insert_dynamic_generic('Philo', 'Viewership', '20251106_103826.csv');
```

**Purpose:**
- INSERT matched records into `episode_details` table
- Set `processed = TRUE` on source records
- Set `phase = '3'`

**Success Criteria:**
- Records appear in `episode_details`
- Unmatched records logged in `record_reprocessing_batch_logs`
- Sum of (matched + unmatched) = expected count

**Verification:**
```javascript
const viewershipCount = await query(`
    SELECT COUNT(*) FROM ASSETS.PUBLIC.EPISODE_DETAILS
    WHERE filename = '${filename}' AND label = 'Viewership'
`);

const unmatchedCount = await query(`
    SELECT COUNT(*) FROM METADATA_MASTER.PUBLIC.record_reprocessing_batch_logs
    WHERE filename = '${filename}'
`);

const totalProcessed = viewershipCount + unmatchedCount;
if (totalProcessed >= expectedCount) {
    // Success!
}
```

## Retry Logic

### Per-Phase Retries
```javascript
const MAX_RETRIES = 3;
let retryCount = 0;

while (retryCount < MAX_RETRIES) {
    try {
        await executePhase(phase);
        if (await verifyPhase(phase)) {
            break;  // Success!
        }
    } catch (error) {
        retryCount++;
        await sleep(5000);  // Wait 5 seconds before retry
    }
}
```

### What Triggers Retry
- SQL execution error (network, timeout, etc.)
- Verification failure (count mismatch)
- Procedure returns error string

### What Doesn't Retry
- Validation failure in Phase 2 (data quality issue, not transient)
- Final verification failure after 3 attempts (something structurally wrong)

## Verification Strategy

### Phase 1 Verification
**Check:** Records have `deal_parent` set
```sql
SELECT COUNT(*) FROM platform_viewership
WHERE filename = '{filename}'
  AND deal_parent IS NOT NULL;
```

### Phase 2 Verification
**Check 1:** Count in `platform_viewership` matches expected
```sql
SELECT COUNT(*) FROM platform_viewership
WHERE platform = '{platform}'
  AND processed IS NULL
  AND phase = '2'
  AND filename = '{filename}';
```

**Check 2:** Validation passes
```javascript
const validationResult = await callProcedure('validate_viewership_for_insert', ...);
const validation = JSON.parse(validationResult);
if (!validation.valid) {
    throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
}
```

### Phase 3 Verification
**Check:** Matched + Unmatched = Expected
```javascript
const matched = await queryCount(`
    SELECT COUNT(*) FROM episode_details
    WHERE filename = '{filename}' AND label = '{dataType}'
`);

const unmatched = await queryCount(`
    SELECT COUNT(*) FROM record_reprocessing_batch_logs
    WHERE filename = '{filename}'
`);

if (matched + unmatched >= expectedCount) {
    return true;
} else {
    console.error(`Count mismatch: expected ${expectedCount}, got ${matched + unmatched}`);
    return false;
}
```

**Why >= instead of ==?**
- Platform may send duplicate records
- Better to process all than fail on a few extras
- Unmatched records might exist from previous attempts

## Error Handling

### Graceful Failures
```javascript
try {
    await phase1();
} catch (error) {
    console.error('Phase 1 failed:', error);
    await sendEmail({
        subject: 'Phase 1 Failed',
        body: `Error: ${error.message}\nPlatform: ${platform}\nFilename: ${filename}`
    });
    throw error;  // Lambda will mark as failed
}
```

### Email Notifications

**On Success:**
```
Subject: Viewership Processing Complete - Philo
Body:
- Filename: 20251106_103826.csv
- Expected: 100 records
- Matched: 52 records
- Unmatched: 48 records
- Status: SUCCESS
```

**On Failure:**
```
Subject: Viewership Processing FAILED - Philo
Body:
- Filename: 20251106_103826.csv
- Phase: 2
- Error: Validation failed - missing deal_parent for 25 records
- Retry count: 3
- Status: FAILED
```

### Logging Strategy

All logs go to CloudWatch Logs:

```javascript
console.log('INFO', 'Phase 2 starting...');
console.log('SQL (viewership):', viewershipQuery);
console.log('Viewership count:', result.rows[0].count);
console.error('ERROR', 'Phase verification failed');
```

**Log Levels:**
- `INFO` - Normal flow
- `WARNING` - Recoverable issue (retry)
- `ERROR` - Unrecoverable issue (fail)
- `DEBUG` - Detailed query/result logging

## Connection Management

### Snowflake Connection Pool
```javascript
const snowflake = require('snowflake-sdk');

let connection = null;

async function getConnection() {
    if (connection) {
        console.log('Reusing existing connection');
        return connection;
    }

    console.log('Creating new connection');
    connection = snowflake.createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USER,
        password: process.env.SNOWFLAKE_PASSWORD,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE,
        database: process.env.SNOWFLAKE_DATABASE,
        schema: 'PUBLIC',
        role: 'WEB_APP'
    });

    await connection.connect();
    return connection;
}
```

**Connection Reuse:**
- Lambda containers warm for ~15 minutes
- Reusing connection saves ~2-3 seconds per phase
- Connection closes when Lambda container dies

### Error: "Connection already closed"
**Cause:** Lambda container reused but connection died
**Solution:** Check connection health before reuse:
```javascript
if (connection && !connection.isUp()) {
    connection = null;  // Force reconnect
}
```

## Environment Variables

### Required
```bash
SNOWFLAKE_ACCOUNT=xyz12345.us-east-1
SNOWFLAKE_USER=lambda_user
SNOWFLAKE_PASSWORD=<from Secrets Manager>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=UPLOAD_DB_PROD
SNOWFLAKE_ROLE=WEB_APP

SES_FROM_EMAIL=notifications@example.com
SES_TO_EMAIL=ops-team@example.com
SES_REGION=us-east-1
```

### Optional
```bash
DEBUG_MODE=true  # Enable verbose logging
SKIP_EMAIL=true  # Don't send emails (testing)
```

## Performance Characteristics

### Cold Start
- ~3-5 seconds to initialize Lambda
- ~2-3 seconds to establish Snowflake connection
- Total: ~5-8 seconds before first query

### Warm Start
- ~0.5 seconds to resume Lambda
- Connection already established
- Total: ~0.5 seconds before first query

### Typical Execution Times
- **Phase 1:** 10-30 seconds
- **Phase 2:** 30-120 seconds (depends on content matching)
- **Phase 3:** 10-30 seconds
- **Total:** 1-3 minutes for 100 records

### Timeout Considerations
- Default timeout: 15 minutes
- Large batches (10k+ records) may exceed
- Solution: Split into smaller files or increase timeout

## Common Issues & Solutions

### Issue: Lambda timeout
**Cause:** Large batch (10k+ records) or slow Snowflake warehouse
**Solution:**
- Increase Lambda timeout to 15 minutes (max)
- Increase Snowflake warehouse size
- Split file into smaller batches

### Issue: "Validation failed - missing deal_parent"
**Cause:** Partner not in active_deals table
**Solution:**
- Add partner to active_deals
- Or set fallback territory/channel manually
- Reprocess file

### Issue: Count mismatch in Phase 3
**Cause:** Unmatched records not logged in `record_reprocessing_batch_logs`
**Solution:**
- Check if orchestrator's INSERT to logs succeeded
- Manually log unmatched records
- **Fixed 2025-11-06:** Orchestrator now correctly logs unmatched records

### Issue: Email not sent
**Cause:** SES not configured or Lambda lacks IAM permissions
**Solution:**
```json
{
  "Effect": "Allow",
  "Action": ["ses:SendEmail", "ses:SendRawEmail"],
  "Resource": "*"
}
```

## Testing

### Local Test (with AWS SAM)
```bash
sam local invoke \
  --event test-events/philo-upload.json \
  --env-vars env.json
```

### Integration Test
1. Upload small test file via Streamlit
2. Check CloudWatch Logs for Lambda execution
3. Verify final count in `episode_details`
4. Check email received

### Unit Test
```javascript
// Mock Snowflake connection
const mockConnection = {
    execute: jest.fn((sql, callback) => {
        callback(null, {rows: [{count: 100}]});
    })
};

test('Phase 2 verification passes with correct count', async () => {
    const result = await verifyPhase2(mockConnection, 'Philo', 'test.csv', 100);
    expect(result).toBe(true);
});
```

## Monitoring

### CloudWatch Metrics
- **Invocations:** Count of Lambda executions
- **Errors:** Count of failed executions
- **Duration:** Execution time (ms)
- **Throttles:** Count of throttled invocations (should be 0)

### Custom Metrics
Add using CloudWatch Embedded Metric Format:
```javascript
console.log(JSON.stringify({
    _aws: {
        Timestamp: Date.now(),
        CloudWatchMetrics: [{
            Namespace: 'ViewershipPipeline',
            Dimensions: [['Platform']],
            Metrics: [{Name: 'ProcessingTime', Unit: 'Milliseconds'}]
        }]
    },
    Platform: 'Philo',
    ProcessingTime: 45000
}));
```

### Alarms
Set CloudWatch Alarms on:
- Error rate > 5%
- Duration > 10 minutes (approaching timeout)
- Invocations = 0 for 24 hours (pipeline broken?)

## Deployment

### Via AWS Console
1. Update `lambda/index.js`
2. Zip: `cd lambda && zip -r function.zip . && cd ..`
3. Upload to Lambda console

### Via AWS CLI
```bash
aws lambda update-function-code \
  --function-name register-start-viewership-data-processing \
  --zip-file fileb://lambda/function.zip \
  --region us-east-1
```

### Via CI/CD (Recommended)
```yaml
# .github/workflows/deploy-lambda.yml
- name: Deploy Lambda
  run: |
    cd lambda
    npm install --production
    zip -r function.zip .
    aws lambda update-function-code \
      --function-name ${{ secrets.LAMBDA_FUNCTION_NAME }} \
      --zip-file fileb://function.zip
```

## Change History

### 2025-11-06
- Phase 3 verification now includes unmatched records from `record_reprocessing_batch_logs`
- Fixed count mismatch issue

### Earlier
- Original 3-phase implementation
- Added email notifications
- Added retry logic with backoff
