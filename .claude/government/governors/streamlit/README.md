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
- **Form Delegate** - Upload form and dropdowns
- **Validation Delegate** - User input validation
- **Error Display Delegate** - Showing errors and results to user

## Critical Red Lines

To be established in Constitutional Convention.

**Potential Red Lines:**
- When `processed` flag is set to TRUE
- Date format auto-detection logic
- Required vs. optional columns
- Data validation before upload

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
