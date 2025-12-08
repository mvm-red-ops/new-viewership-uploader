# Deployment Verification System

## The Problem We Solved

**Before:** No visibility into deployment status. We deployed procedures but had no way to know:
- If all procedures actually got created
- If template substitution worked
- If permissions were granted
- If bucket procedures were linked correctly

**Result:** Silent failures. Asset matching failed but we didn't know why until manually debugging.

## The Solution: 3 Levels of Verification

### 1. Deployment Check (Existence & Configuration)

Verifies the deployment itself is complete:

```bash
python sql/diagnostics/diagnose.py --env prod --check deployment
```

**What it checks:**
- ✅ **Existence:** All 13 main procedures exist
- ✅ **Existence:** All 6 bucket procedures exist (CRITICAL!)
- ✅ **Existence:** All UDFs exist
- ✅ **Template Substitution:** No `{{PLACEHOLDERS}}` left in code
- ✅ **Permissions:** UDFs, sequences, tables all have correct grants
- ✅ **Health:** Procedures are callable
- ✅ **Linkage:** Main procedure references bucket procedures

**When to use:**
- After every deployment
- When troubleshooting "why isn't it working"
- Before promoting staging → prod

### 2. Environment Comparison (Staging vs Prod)

See differences between environments:

```bash
python sql/diagnostics/compare.py
```

**What it shows:**
- Procedures only in staging
- Procedures only in prod
- Critical bucket procedures status in both
- UDF differences

**When to use:**
- Before deploying to prod
- When prod behaves differently than staging
- To verify parity

### 3. Data Flow Check (Runtime Behavior)

Verify actual data processing works:

```bash
python sql/diagnostics/diagnose.py --env prod --platform Roku --filename "file.csv"
```

**What it checks:**
- Upload success
- Deal matching worked
- Internal series matching worked
- Asset matching worked (or how many unmatched)
- Unmatched records were logged
- No blocking errors

**When to use:**
- After an upload completes
- When Lambda verification fails
- To understand why records didn't insert

## Deployment Workflow

### Single Command Deployment

```bash
# Deploy everything to staging
python sql/deploy/deploy.py --env staging

# Deploy everything to prod
python sql/deploy/deploy.py --env prod
```

**This automatically:**
1. Deploys all migrations in order
2. Replaces template placeholders
3. Runs verification checks
4. Reports any issues

### Verify After Deployment

```bash
# Check staging deployment
python sql/diagnostics/diagnose.py --env staging --check deployment

# Check prod deployment
python sql/diagnostics/diagnose.py --env prod --check deployment
```

### Compare Environments

```bash
# See what's different between staging and prod
python sql/diagnostics/compare.py
```

## What Gets Caught

### ❌ Missing Bucket Procedures
```
Bucket Procedures (UPLOAD_DB_PROD.PUBLIC):
  ❌ process_viewership_series_only_generic MISSING
  ❌ process_viewership_full_data_generic MISSING

  ⚠️  CRITICAL: Bucket procedures missing - asset matching WILL FAIL
```

### ❌ Template Substitution Failed
```
Template Substitution Checks:
  ❌ Unsubstituted placeholders found:
     - {{METADATA_DB}}
     - {{STAGING_DB}}

  💡 Procedures were deployed without template substitution!
```

### ❌ Missing Permissions
```
Sequence Permissions:
  ❌ WEB_APP missing USAGE on RECORD_REPROCESSING_IDS
     ⚠️  CRITICAL: Unmatched logging WILL FAIL without this!
```

### ❌ Broken Linkage
```
Bucket Procedure Linkage:
  ❌ Main procedure does NOT reference bucket procedures
     Asset matching will fail silently!
```

## Emergency Commands

### "Nothing is working in prod"
```bash
# 1. Check what's deployed
python sql/diagnostics/diagnose.py --env prod --check deployment

# 2. Compare to staging
python sql/diagnostics/compare.py

# 3. Redeploy everything
python sql/deploy/deploy.py --env prod
```

### "Asset matching failed"
```bash
# Check deployment first
python sql/diagnostics/diagnose.py --env prod --check deployment

# If deployment is good, check data flow
python sql/diagnostics/diagnose.py --env prod --platform Roku --filename "file.csv"
```

### "Are staging and prod the same?"
```bash
python sql/diagnostics/compare.py
```

## Integration with Deployment

The deployment script (`sql/deploy/deploy.py`) **automatically** runs deployment verification unless you use `--skip-verify`:

```bash
# This will auto-verify
python sql/deploy/deploy.py --env prod

# This skips verification
python sql/deploy/deploy.py --env prod --skip-verify
```

**After deployment, you'll see:**
```
================================================================================
🔍 Running Comprehensive Deployment Verification
================================================================================

1. EXISTENCE CHECKS
--------------------------------------------------------------------------------
Main Procedures (UPLOAD_DB_PROD.PUBLIC):
  ✅ set_phase_generic
  ✅ calculate_viewership_metrics
  ...

Bucket Procedures (UPLOAD_DB_PROD.PUBLIC):
  ✅ process_viewership_full_data_generic
  ...

2. TEMPLATE SUBSTITUTION CHECKS
--------------------------------------------------------------------------------
  ✅ Template substitution successful

3. PERMISSIONS CHECKS
--------------------------------------------------------------------------------
  ✅ WEB_APP has USAGE on EXTRACT_PRIMARY_TITLE
  ✅ WEB_APP has USAGE on RECORD_REPROCESSING_IDS

4. HEALTH CHECKS
--------------------------------------------------------------------------------
  ✅ set_phase_generic is callable
  ✅ Main procedure references bucket procedures

================================================================================
✅ ALL DEPLOYMENT CHECKS PASSED
   System is fully deployed and operational
================================================================================
```

## Summary

**One deployment script for both environments:**
```bash
python sql/deploy/deploy.py --env staging  # or --env prod
```

**Three levels of verification:**
1. `--check deployment` - Is everything in place?
2. `compare.py` - Are staging and prod the same?
3. `--check data-flow` - Is data processing working?

**No more:**
- ❌ Silent failures
- ❌ "Why didn't bucket procedures deploy?"
- ❌ "Are we missing permissions?"
- ❌ "Is staging the same as prod?"

**Now we have:**
- ✅ Automatic verification after deployment
- ✅ Clear error messages with solutions
- ✅ Comparison between environments
- ✅ Health checks at multiple levels
- ✅ Single source of truth for what should be deployed
