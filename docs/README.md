# Documentation Index

Documentation for the viewership uploader system.

## Active Documentation

### Core Guides
- **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - How to deploy stored procedures to staging/prod
- **[ASSET_MATCHING_ARCHITECTURE.md](ASSET_MATCHING_ARCHITECTURE.md)** - How the content matching system works
- **[SERVICE_BUCKET_PROCEDURES.md](SERVICE_BUCKET_PROCEDURES.md)** - Bucket procedure reference
- **[SERVICE_LAMBDA.md](SERVICE_LAMBDA.md)** - Lambda orchestration details
- **[SERVICE_ORCHESTRATOR.md](SERVICE_ORCHESTRATOR.md)** - Main orchestrator procedure

### Recent Fixes
- **[FIXES_2025_12_02.md](FIXES_2025_12_02.md)** - December 2025 fixes
  - Date fields going NULL for monthly/quarterly data
  - asset_series not being set for REF_ID_SERIES bucket

## DEPLOYMENT_GUIDE.md

Quick reference for deploying procedures:
- Individual deployment scripts (deploy_ref_id_series_proc.py, etc.)
- Environment configuration
- Testing after deployment
- Rollback procedures
- Best practices

Use when: Deploying fixes, setting up environments, troubleshooting deployments

## FIXES_YYYY_MM_DD.md

Detailed changelog of specific fixes with:
- Problem symptoms and root cause
- Code changes (before/after)
- Test results
- Impact assessment
- Verification commands

Use when: Understanding changes, troubleshooting similar issues, reviewing history

## Adding New Documentation

When making significant changes to stored procedures:

1. Create a new `FIXES_YYYY_MM_DD.md` file documenting:
   - What problem you were solving
   - What the root cause was
   - What files you changed and why
   - How to verify the fix works
   - What the impact is (which platforms/data types affected)

2. Update `DEPLOYMENT_GUIDE.md` if you:
   - Add a new deployment script
   - Change the deployment process
   - Add new template variables
   - Change environment configuration

3. Keep this README.md updated with new documentation files

## File Naming Conventions

- `FIXES_YYYY_MM_DD.md` - Fixes applied on specific date
- `DEPLOYMENT_GUIDE.md` - General deployment instructions (not date-specific)
- `README.md` - This file, index of all documentation

## Related Documentation

Outside this directory:

- `../sql/migrations/` - Database schema migration scripts
- `../snowflake/stored_procedures/` - Source code for all stored procedures
- `../README.md` - Project overview (if it exists)

## Common Questions

### Where do I find the stored procedure source code?
`../snowflake/stored_procedures/`

### How do I deploy a procedure?
See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

### What changed recently?
Check the most recent `FIXES_YYYY_MM_DD.md` file

### How do I test after deployment?
See "Testing After Deployment" section in [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

### How do I rollback a deployment?
See "Rollback" section in [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)

## Documentation Standards

When writing documentation:

1. **Be specific** - Include exact file paths, line numbers, SQL queries
2. **Show before/after** - Include code snippets showing what changed
3. **Include test results** - Show actual data proving the fix works
4. **Explain the why** - Not just what you did, but why it was necessary
5. **Add verification steps** - How to confirm the fix is working

## Maintenance

Review and update documentation:
- After every significant fix or deployment
- When procedures are refactored
- When new features are added
- When deployment process changes

Keep documentation in sync with code changes.
