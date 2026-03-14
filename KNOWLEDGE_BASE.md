# Viewership Uploader - Knowledge Base

## Overview

The Viewership Uploader is a Streamlit application for processing and uploading video viewership and revenue data to Snowflake. It supports multiple platforms (YouTube, Roku, Philo, Wurl, Freevee, etc.) with flexible template-based column mapping.

## Architecture

```
viewership-uploader/
├── app.py                          # Main Streamlit application
├── config.py                       # Configuration and settings
├── lambda/                         # AWS Lambda functions
│   └── snowflake-helpers.js        # Snowflake verification logic
├── snowflake/                      # Snowflake DDL and procedures
│   └── stored_procedures/
├── youtube_api/                    # YouTube Analytics integration
│   ├── mcp-server.py              # MCP server for YouTube data
│   ├── API_SPECS.md               # YouTube API specifications
│   └── scripts/                   # Utility scripts
├── sample_data/                    # Sample data files
│   ├── freevee/
│   ├── viewership/
│   └── rev_by_episode/
└── scripts/                        # Utility scripts
```

## Core Concepts

### 1. Templates

Templates define how to map source CSV columns to target Snowflake schema columns.

**Template Structure**:
```json
{
  "PLATFORM": "YouTube",
  "PARTNER": "DEFAULT",
  "CHANNEL": "",
  "TERRITORIES": [],
  "DOMAIN": "",
  "DATA_TYPE": "Viewership",
  "column_mappings": {
    "target_column": {
      "source_column": "source_name"
    },
    "hardcoded_column": {
      "hardcoded_value": "Fixed Value"
    }
  }
}
```

**Storage**: Templates are stored in `dictionary.public.viewership_file_formats` table.

**Key Fields**:
- **Platform**: YouTube, Roku, Philo, Wurl, Freevee, etc.
- **Partner**: Partner identifier or "DEFAULT" for platform-wide templates
- **Channel**: Specific channel (optional)
- **Territories**: List of territories (e.g., ["US", "CA"])
- **Domain**: SVOD/AVOD/Linear classification
- **Data Type**: "Viewership", "Revenue", or "Viewership_Revenue"

### 2. Data Types

Three types of data uploads:

1. **Viewership** - Hours/minutes by episode
   - Maps to: `DICTIONARY.STAGING.EPISODIC_VIEWERSHIP`
   - Key metrics: hours_watched, views, date

2. **Revenue** - Revenue by episode
   - Maps to: `DICTIONARY.STAGING.EPISODIC_REVENUE`
   - Key metrics: revenue, date, episode

3. **Viewership_Revenue** - Combined metrics
   - Maps to both staging tables
   - Splits data based on column mappings

### 3. Column Mapping Types

**Source Column Mapping**:
```json
{
  "title": {
    "source_column": "Video title"
  }
}
```
Maps a source CSV column to target schema column.

**Hardcoded Value**:
```json
{
  "partner": {
    "hardcoded_value": "Philo"
  }
}
```
Sets a fixed value for all rows.

**Derived Values**:
Some columns are automatically derived:
- `is_short`: Calculated from duration (≤ 60 seconds)
- `avg_view_duration_seconds`: Calculated from hours_watched / views
- `minutes_watched`: Calculated from hours_watched × 60

### 4. Upload Pipeline

```
CSV File → Parse → Map Columns → Transform → Snowflake Staging → Lambda Verification → Production
```

**Steps**:
1. **Upload CSV**: User uploads file via Streamlit
2. **Detect Format**: App tries to detect platform from filename/content
3. **Select Template**: User selects or searches for template
4. **Map Columns**: Automatic or manual column mapping
5. **Transform Data**: Apply mappings, calculate derived fields
6. **Upload to Staging**: Insert into Snowflake staging tables
7. **Lambda Verification**: Verify record counts and data integrity
8. **Move to Production**: Lambda moves verified data to production tables

### 5. Template Hierarchy

Templates are matched in this order:

1. **Partner + Channel + Territory** - Most specific
2. **Partner + Channel** - No territory specified
3. **Partner + Territory** - No channel specified
4. **Partner only** - No channel or territory
5. **Platform DEFAULT** - Fallback template

**Multi-Territory Support**:
- Single upload can target multiple territories
- Template lookup handles each territory separately
- Falls back to DEFAULT if territory-specific template missing

## Platform-Specific Details

### YouTube

**Template Columns**:
- avg_view_duration_seconds
- date
- duration_seconds
- hours_watched
- is_short
- minutes_watched
- published_date
- title
- url
- video_id
- views

**Data Fetching**:
- Use `fetch_youtube_q4_2025_daily_all_metrics.py`
- Fetches daily breakdown with all metrics
- Rate limited to ~2 requests/sec
- Takes 15-20 minutes for 1,200 videos

**Special Features**:
- Automatic URL construction from video_id
- is_short detection based on duration
- avg_view_duration calculated from watch time / views

### Freevee

**Key Channels**:
- Nosey
- Confess by Nosey
- Judge Nosey
- Presented by Nosey (aggregates all "presented by" channels)

**Aggregation**:
- Source: Minutes Streamed
- Convert to Hours of Viewing (HOV): minutes / 60
- Group by month and channel

**Templates**:
- Freevee Linear
- Freevee VOD

### Roku

**Data Type**: Hours/Mins by Episode (Viewership)

**Common Files**:
- Roku Nosey Data (US/CA)
- Roku Confess Nosey Data (US/CA)
- Roku JudgeNosey Data (US/CA)

### Philo

**Special Handling**:
- Partner hardcoded to "Philo"
- Viewership-only uploads (no revenue)
- Lambda verification checks type parameter

### Wurl

**Special Handling**:
- Channel name hardcoded (not from CSV column)
- Multi-channel support
- Territory-based templates

## Database Schema

### Staging Tables

**EPISODIC_VIEWERSHIP**:
```sql
CREATE TABLE DICTIONARY.STAGING.EPISODIC_VIEWERSHIP (
    date DATE,
    partner VARCHAR,
    channel VARCHAR,
    territory VARCHAR,
    title VARCHAR,
    episode_number INT,
    season_number INT,
    hours_watched FLOAT,
    views INT,
    domain VARCHAR,
    -- ... additional columns
);
```

**EPISODIC_REVENUE**:
```sql
CREATE TABLE DICTIONARY.STAGING.EPISODIC_REVENUE (
    date DATE,
    partner VARCHAR,
    channel VARCHAR,
    territory VARCHAR,
    title VARCHAR,
    episode_number INT,
    season_number INT,
    revenue FLOAT,
    -- ... additional columns
);
```

### Template Table

**viewership_file_formats**:
```sql
CREATE TABLE DICTIONARY.PUBLIC.VIEWERSHIP_FILE_FORMATS (
    platform VARCHAR,
    partner VARCHAR,
    channel VARCHAR,
    territory VARCHAR,
    domain VARCHAR,
    data_type VARCHAR,
    column_mappings VARIANT,
    sample_data VARIANT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

## Lambda Functions

### snowflake-helpers.js

**Purpose**: Verify staged data and move to production

**Key Functions**:

1. **verifyPhase**: Check record counts match expected
   - Handles Viewership, Revenue, and combined uploads
   - Validates based on `type` parameter

2. **moveToProduction**: Move verified data to production tables

**Verification Logic**:
```javascript
const uploadType = type?.toLowerCase()?.trim() ?? "";
const expectsViewership = uploadType.includes("viewership");
const expectsRevenue = uploadType.includes("revenue");

// Only check counts for expected data types
if (expectsViewership) {
    verify viewership count matches file_record_count
}
if (expectsRevenue) {
    verify revenue count matches file_record_count
}
```

## Common Operations

### Creating a New Template

1. Go to "Create Template" tab
2. Upload sample CSV file
3. Fill in platform, partner, channel, territories
4. Map columns (source or hardcoded)
5. Save template

### Editing an Existing Template

1. Go to "Search & Edit" tab
2. Search for template by platform/partner/channel
3. Click "Edit"
4. Modify mappings or metadata
5. Upload new sample file if needed
6. Save changes

### Uploading Data

1. Go to "Upload & Map" tab
2. Upload CSV file
3. Select platform, partner, channel, territories
4. Verify column mappings
5. Preview transformed data
6. Click "Upload to Snowflake"

### Aggregating Data

For aggregations (like Freevee HOV by month):
1. Use utility scripts (e.g., `aggregate_freevee_hov.py`)
2. Process sample data files
3. Generate aggregated CSV
4. Upload via standard template

## Troubleshooting

### "Column not found" Error

**Cause**: Template expects source column that doesn't exist in CSV

**Fix**:
- Change template to use hardcoded_value instead
- Or update CSV to include missing column

### "Record count mismatch" in Lambda

**Cause**: Lambda verification failed

**Common Issues**:
- Viewership-only upload but Lambda expects revenue
- Revenue-only upload but Lambda expects viewership

**Fix**:
- Check `type` parameter matches data being uploaded
- Update Lambda verification logic if needed

### Template Not Found

**Cause**: No matching template for platform/partner/channel/territory combination

**Fix**:
- Create missing template
- Or select DEFAULT template for platform
- Check territory spelling

### OAuth Token Expired (YouTube)

**Cause**: YouTube OAuth token expires after 7 days

**Fix**:
```bash
cd youtube_api
rm token.pickle
python scripts/setup_oauth.py
```

## Best Practices

### Templates

1. **Use DEFAULT templates** for platform-wide settings
2. **Create specific templates** only when needed (different column names)
3. **Store sample data** in templates for reference
4. **Document hardcoded values** in template descriptions

### Data Upload

1. **Verify data quality** before upload (check for nulls, invalid dates)
2. **Use preview** to verify transformations
3. **Check Snowflake staging** after upload
4. **Monitor Lambda logs** for verification errors

### YouTube Data

1. **Fetch daily breakdown** for trend analysis
2. **Use aggregated data** for summary reports
3. **Cache results** to avoid re-fetching
4. **Respect rate limits** (0.5s between requests)

### Development

1. **Test templates** with small sample files first
2. **Use dev environment** for testing
3. **Document custom mappings** in CLAUDE.md or comments
4. **Version control templates** by exporting to JSON

## MCP Integration

The app supports Model Context Protocol (MCP) for AI agent integration.

### YouTube Analytics MCP

**Server**: `youtube_api/mcp-server.py`

**Tools**:
- `fetch_channel_summary`: Get channel-level analytics
- `fetch_video_analytics`: Get video-specific analytics
- `fetch_top_videos`: Get top performing videos
- `fetch_daily_trends`: Get daily trend data

**Usage**:
```bash
cd youtube_api
python mcp-server.py
```

**Configuration**: See `youtube_api/mcp-config.json`

## API References

### Snowflake

- Connection via environment variables or secrets.toml
- Uses `snowflake-connector-python`
- Staging tables in DICTIONARY.STAGING schema
- Production tables in respective schemas

### YouTube

- YouTube Data API v3
- YouTube Analytics API v2
- OAuth 2.0 authentication
- See `youtube_api/API_SPECS.md` for details

### AWS Lambda

- Node.js 18.x runtime
- Snowflake connector for verification
- Event-driven from Streamlit uploads
- Logs to CloudWatch

## Maintenance

### Regular Tasks

1. **Clean up staging tables** (weekly)
   ```sql
   DELETE FROM DICTIONARY.STAGING.EPISODIC_VIEWERSHIP
   WHERE created_at < DATEADD(day, -7, CURRENT_TIMESTAMP);
   ```

2. **Archive old templates** (monthly)
   - Export unused templates to JSON
   - Mark as inactive or delete

3. **Refresh OAuth tokens** (as needed)
   - YouTube: every 7 days
   - Snowflake: based on policy

4. **Monitor Lambda costs** (monthly)
   - Review CloudWatch logs
   - Optimize verification queries

### Backup

- **Templates**: Export from viewership_file_formats table
- **Sample data**: Keep in git (sample_data/)
- **Scripts**: Version control all Python scripts

## Future Enhancements

- [ ] Batch upload support (multiple files)
- [ ] Automated template detection from CSV structure
- [ ] Real-time validation during upload
- [ ] Template version history
- [ ] API endpoints for programmatic access
- [ ] Scheduled YouTube data fetches
- [ ] Dashboard for upload history
- [ ] Data quality reports
