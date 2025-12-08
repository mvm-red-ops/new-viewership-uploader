# Archive: Pre-Government Cleanup (2025-12-05)

This directory contains files archived before implementing the MCP Government architecture.

## What Was Archived

### Root Directory (`root/`)
- `cleanup_tubi_test.sql` - One-off test script for Tubi data cleanup
- `drop_old_validation.sql` - One-off migration script
- `fix_tubi_data.sql` - One-off data fix script

### Snowflake Scripts (`snowflake/`)
- `deploy_prod_direct.py` - One-time workaround deployment script
- `fix_production_database_names.py` - One-time database name migration

### Documentation (`docs/`)
- `DEPLOY_LAMBDA.md` - Old Lambda deployment guide
- `DEPLOYMENT_CHECKLIST.md` - Old deployment checklist
- `DEPLOYMENT_VERIFICATION.md` - Old verification steps
- `PRODUCTION_DEPLOYMENT_SUMMARY.md` - Historical deployment summary
- `READY_TO_DEPLOY.md` - Old deployment readiness doc
- `REVENUE_BY_EPISODE_FIXES.md` - Historical fix documentation

## Canonical Documentation (Kept in Root)

- `ARCHITECTURE.md` - System architecture overview
- `TROUBLESHOOTING.md` - Debugging and problem-solving guide
- `LAMBDA_FIX_REF_ID.md` - Critical ref_id fix documentation
- `DEPLOY_LAMBDA_FINAL.md` - Current Lambda deployment guide
- `CHEATSHEET.md` - Quick reference commands

## Why Archived

These files were moved to reduce root directory bloat before implementing the MCP Government structure. They represent one-off scripts, outdated documentation, and temporary workarounds that are no longer part of the canonical codebase but preserved for historical reference.

## Government Architecture

The new government structure organizes knowledge into domain-specific governors:
- `.claude/government/governors/snowflake/` - Snowflake domain knowledge
- `.claude/government/governors/streamlit/` - Streamlit UI knowledge
- `.claude/government/governors/lambda/` - AWS Lambda orchestration
- `.claude/government/governors/testing/` - Testing and validation

Each governor maintains a knowledge hierarchy with CANONICAL, REFERENCE, and DIAGNOSTIC categories.
