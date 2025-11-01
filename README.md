# Data Template Manager

A Streamlit application for managing data templates and column mappings for viewership data uploads. This app allows operations users to define and store file format definitions as JSON in a Snowflake database.

## Environment Support

The app supports multiple environments with environment-specific configurations:
- 🟢 **Development** - Local development and testing
- 🟡 **Staging** - Pre-production testing
- 🔴 **Production** - Production use

Each environment can have separate databases, Lambda functions, and AWS credentials. See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

## Features

### 1. Upload & Map
- **File Upload**: Upload CSV or Excel files to create column mappings
- **Data Preview**: View uploaded data with column information and sample values
- **Intelligent Mapping**: Automatic column mapping using pattern matching and similarity algorithms
- **Manual Adjustment**: Easily adjust mappings through dropdown selectors
- **Duplicate Detection**: Automatically detects if a configuration already exists for a platform/partner combination
- **Edit Existing**: Load and edit existing configurations

### 2. Search & Edit
- **Search Configurations**: Search by platform and partner, or view all configurations
- **View Details**: Expandable views showing all mapping details
- **Edit**: Load configurations for editing
- **Delete**: Remove configurations no longer needed

## Required Columns

The app maps uploaded file columns to these required fields:
- Platform
- Partner
- Date
- Content Name
- Content ID
- Series
- Total Watch Time (specify Hours or Minutes)

## Optional Columns

Optional columns are organized by category and searchable. Categories include:

- **Metrics**: AVG_DURATION_PER_SESSION, AVG_DURATION_PER_VIEWER, AVG_SESSION_COUNT, SESSIONS, TOT_SESSIONS, UNIQUE_VIEWERS, VIEWS
- **Geo**: CITY, COUNTRY, TERRITORY
- **Device**: DEVICE_ID, DEVICE_NAME, DEVICE_TYPE
- **Content**: EPISODE_NUMBER, LANGUAGE, CONTENT_PROVIDER, REF_ID, SEASON_NUMBER, SERIES_CODE, VIEWERSHIP_TYPE
- **Date**: MONTH, QUARTER, YEAR_MONTH_DAY, YEAR
- **Monetary**: REVENUE

💡 **Tip**: Click the dropdown and start typing to search for fields (e.g., type "session" to find all session-related metrics)

## Installation

### Prerequisites
- Python 3.8 or higher
- Access to a Snowflake account
- Snowflake database and schema created

### Setup Steps

1. **Clone or download the project files**

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Snowflake and AWS credentials**

   Create a file `.streamlit/secrets.toml` (copy from `.streamlit/secrets.toml.example`):
   ```toml
   [snowflake]
   user = "your_username"
   password = "your_password"
   account = "your_account"  # e.g., "xy12345.us-east-1"
   warehouse = "your_warehouse"
   database = "upload_db"
   schema = "public"

   [aws]
   access_key_id = "YOUR_AWS_ACCESS_KEY"
   secret_access_key = "YOUR_AWS_SECRET_KEY"
   region = "us-east-1"
   lambda_task_orchestrator = "register-start-viewership-data-processing"
   ```

4. **Run the application**
   ```bash
   streamlit run app.py
   ```

4. **Set up Snowflake tables**

   Run the SQL scripts in the `sql/` directory:
   ```bash
   # For staging/development:
   # Run: sql/create_platform_viewership.sql

   # For production:
   # Run: sql/create_platform_viewership_prod.sql

   # After loading data, optimize with:
   # Run: sql/optimize_tables.sql
   ```

5. **Run the application**
   ```bash
   streamlit run app.py
   ```

6. **Access the app**

   Open your browser to `http://localhost:8501`

## Database Schema

The app automatically creates a `partner_configs` table in your Snowflake database:

```sql
CREATE TABLE partner_configs (
    config_id VARCHAR(36) PRIMARY KEY,
    platform VARCHAR(255) NOT NULL,
    partner VARCHAR(255) NOT NULL,
    column_mappings VARIANT NOT NULL,
    validation_rules VARIANT,
    filename_pattern VARCHAR(500),
    source_columns VARIANT,
    target_table VARCHAR(255),
    created_date TIMESTAMP_NTZ,
    updated_date TIMESTAMP_NTZ,
    created_by VARCHAR(255),
    UNIQUE (platform, partner)
)
```

## Usage Guide

### Creating a New Configuration

1. Go to the **Upload & Map** tab
2. Enter the **Platform** name (required)
3. Enter the **Partner** name (optional)
   - Leave blank to create a platform-wide template (e.g., all Amagi files)
   - Specify partner for partner-specific mappings (e.g., Amagi + Netflix)
4. (Optional) Enter a filename pattern (regex) to match files
5. **Upload a sample file** (CSV or Excel)
6. Review the **data preview** and **column information**
7. Review the **suggested column mappings**
   - The app intelligently suggests mappings based on column names
   - Adjust any mappings using the dropdown selectors
8. Click **Save Configuration** to store the mapping

### Editing an Existing Configuration

**Option 1: From Search Tab**
1. Go to the **Search & Edit** tab
2. Search for the configuration or view all
3. Click the **Edit** button on the desired configuration
4. Switch to the **Upload & Map** tab to make changes

**Option 2: From Upload Tab**
1. Go to the **Upload & Map** tab
2. Enter the Platform and Partner
3. Upload a file
4. If a configuration exists, click **Load Existing Configuration**
5. Adjust the mappings as needed
6. Click **Save Configuration** to update

### Searching Configurations

1. Go to the **Search & Edit** tab
2. Choose search method:
   - **Platform & Partner**: Enter search terms (partial match supported)
   - **View All**: See all configurations
3. Click **Search** or view results
4. Expand any configuration to see details

### Deleting a Configuration

1. Go to the **Search & Edit** tab
2. Find the configuration to delete
3. Click the **Delete** button
4. Confirm the deletion

## Project Structure

```
.
├── app.py                          # Main Streamlit application
├── config.py                       # Environment configuration
├── snowflake_utils.py              # Snowflake database operations
├── column_mapper.py                # Intelligent column mapping logic
├── requirements.txt                # Python dependencies
├── sql/                            # SQL scripts for table setup
│   ├── create_platform_viewership.sql       # Staging table
│   ├── create_platform_viewership_prod.sql  # Production table
│   └── optimize_tables.sql                  # Performance optimization
├── .streamlit/
│   ├── secrets.toml                # Your credentials (gitignored)
│   └── secrets.toml.example        # Example configuration
├── README.md                       # This file
├── DEPLOYMENT.md                   # Deployment guide
└── ENVIRONMENTS.md                 # Environment switching guide
```

## Column Mapping Intelligence

The app uses multiple strategies to suggest column mappings:

1. **Pattern Matching**: Matches source columns against predefined patterns for each required field
2. **String Similarity**: Uses sequence matching to find similar column names
3. **Word-Level Matching**: Identifies common words between source and target columns
4. **Exact Match Bonus**: Prioritizes exact matches

### Mapping Patterns

The app recognizes various naming conventions for each required field:

- **Platform**: platform, service, streaming_service, provider
- **Partner**: partner, content_partner, studio, distributor
- **Channel**: channel, network, station, outlet
- **Territory**: territory, region, country, market, geography
- **Date**: date, period, week, month, timestamp, time
- **Content Name**: content_name, asset_name, program_name, episode_name, title
- **Content ID**: content_id, asset_id, program_id, show_id, video_id
- **Series**: series, show, program, program_name, series_name
- **Total Watch Time**: hours, minutes, viewership, viewing_hours, viewing_minutes, watch_hours, watch_minutes, watch_time, duration, total_time

## Security Notes

- **Never commit** `.streamlit/secrets.toml` to version control
- Add `.streamlit/secrets.toml` to your `.gitignore` file
- Use environment-specific credentials
- Regularly rotate Snowflake passwords
- Use Snowflake roles with minimum required permissions

## Troubleshooting

### Connection Issues
- Verify Snowflake credentials in `.streamlit/secrets.toml`
- Ensure your Snowflake account is accessible
- Check warehouse is running
- Verify database and schema exist

### Table Creation Issues
- Ensure your Snowflake user has CREATE TABLE permissions
- Check the schema is correct in your configuration

### File Upload Issues
- Supported formats: CSV, XLSX, XLS
- Ensure file has column headers
- Check file encoding (UTF-8 recommended)

## Future Enhancements

Potential features to add:
- Validation rules editor
- Bulk import/export of configurations
- Configuration versioning
- Data quality checks
- Automated file processing
- Email notifications
- Audit logging

## Support

For issues or questions:
1. Check the Troubleshooting section
2. Review Snowflake connection settings
3. Verify file format compatibility
