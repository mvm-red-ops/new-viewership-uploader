# Streamlit Governor

## Domain

The Streamlit Governor owns all aspects of the web application:
- User interface and data upload forms
- Data transformation and validation
- File parsing and column mapping
- Integration with Lambda/Snowflake
- User feedback and error display

## Status

**AWAITING CONSTITUTIONAL CONVENTION**

## Delegates

### Data Upload
- **File Upload Delegate** - CSV file parsing and validation
- **Column Mapping Delegate** - Mapping user columns to system columns
- **Transformation Delegate** - Data transformations and date parsing

### Integration
- **Lambda Trigger Delegate** - Triggering Lambda for procedure execution
- **Database Write Delegate** - Writing to UPLOAD_DB.platform_viewership

### User Interface
- **Form Delegate** - Upload form and dropdowns (including multi-territory support)
- **Validation Delegate** - User input validation
- **Error Display Delegate** - Showing errors and results to user

## Critical Red Lines

To be established in Constitutional Convention.

**Potential Red Lines:**
- When `processed` flag is set to TRUE
- Date format auto-detection logic
- Required vs. optional columns
- Data validation before upload
- Territory array handling and validation
- Template configuration storage format

## Recent Updates

### Multi-Territory Support (Dec 8, 2025)
**UI Changes:**
- Territory selection changed from single-select dropdown to multi-select
- Users can now select multiple territories for a single template
- Widget state managed separately from session_state to prevent caching issues
- Widget keys: `territories_widget` (create mode), `territories_widget_edit` (edit mode)

**Available Territories:**
United States, Canada, India, Mexico, Australia, New Zealand, International, Brazil, Latin America, Sweden, Norway, Denmark, United Kingdom

**Bug Fixes:**
1. **Multiselect Caching Issue** (app.py:1274-1289, 1456-1472): Fixed by using separate widget keys while managing `st.session_state.selected_territories` separately
2. **Nested Hardcoded Value Bug** (app.py:1709-1720, 1877-1883, 2652-2654): Fixed by unwrapping nested `hardcoded_value` dictionaries BEFORE saving (preventing accumulation) and on load (cleaning existing nested values). This prevents configs from accumulating wrapper layers each time they're saved.

**Database Changes:**
- `COLUMN_MAPPING_CONFIGS.territories` changed from VARCHAR to ARRAY
- Unique constraint updated to `(platform, partner, channel, territories)`

### Revenue Data Handling (Dec 8, 2025)
**Data Cleaning:**
- Revenue columns with currency formatting (`$ 0.01`, `$ -`, etc.) are now automatically cleaned before upload
- Currency symbols ($), commas, and spaces are stripped during transformation (app.py:2479-2481)

**Zero-Revenue Filtering:**
- Records with zero or empty revenue are filtered out BEFORE database load (app.py:2475-2492)
- Filtered values include: NULL, empty string, "-", "0", "0.0"
- User feedback shows: "Filtered out X zero-revenue records. Loading Y records."
- This reduces storage and processing overhead for revenue-based uploads

**Impact:**
- Pluto LatAm and similar revenue files with `$ -` values now upload successfully
- Only non-zero revenue records are loaded to database
- No more "Numeric value '$ 0.01' is not recognized" errors

## Communication Protocol

```
Streamlit Delegates → Streamlit Governor: Always allowed
Streamlit Governor → President: Always allowed
Streamlit Delegate → President: Requires Streamlit Governor approval OR 2+ delegate consensus
```

## Decision Authority

**Streamlit Governor CAN approve:**
- UI/UX improvements
- New transformation options
- Validation message updates
- Documentation updates

**Streamlit Governor CANNOT approve alone:**
- Changes to processed flag logic (requires Snowflake Governor)
- Changes to Lambda integration (requires Lambda Governor)
- Breaking changes to column names (requires Snowflake Governor)

## Constitutional Convention Questions

### Data Upload Flow
- [ ] What is the exact sequence of events during upload?
- [ ] When should `processed` flag be set to TRUE vs. FALSE?
- [ ] What validations must happen before writing to UPLOAD_DB?
- [ ] How should upload errors be displayed to users?

### Transformations
- [ ] What transformations are auto-applied vs. user-configured?
- [ ] How does date format auto-detection work?
- [ ] When is it safe to modify transformation logic?
- [ ] What happens if transformation fails?

### Integration
- [ ] When does Streamlit trigger Lambda?
- [ ] What happens if Lambda fails?
- [ ] How does Streamlit know when processing is complete?
- [ ] What feedback is shown to users during processing?

### Columns & Mapping
- [ ] What columns are required vs. optional?
- [ ] How are platform-specific columns handled?
- [ ] What happens if user doesn't map required columns?
- [ ] Can column names in UPLOAD_DB be changed?

### User Experience
- [ ] What level of detail should errors show?
- [ ] How should success be communicated?
- [ ] What guidance should be provided during upload?
- [ ] When should users be warned vs. blocked?

## Delegate Knowledge Base

To be built after Constitutional Convention.

## Next Steps

- [ ] Hold Constitutional Convention with Citizen
- [ ] Document upload flow step-by-step
- [ ] Map integration points with Lambda/Snowflake
- [ ] Catalog all transformations and their logic
- [ ] Create testing checklist for UI changes
