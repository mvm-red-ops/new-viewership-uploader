# Diagnostic Cheatsheet

## One Command For Everything

```bash
./check                          # Is everything healthy?
./check prod                     # Is prod healthy?
./check status prod              # What uploaded recently?
./check compare                  # Are staging/prod in sync?
./check fix                      # How do I fix issues?

./check prod Roku file.csv       # Why did this upload fail?
```

## Deploy

```bash
# Deploy everything
python sql/deploy/deploy.py --env prod

# Auto-verifies. If issues:
./check deploy prod              # See what's wrong
./check fix                      # Get fix commands
```

## Common Issues → One-Line Fixes

### "Asset matching failed - 0 matched"
```bash
./check deploy prod              # Check if bucket procedures exist
# If missing: python sql/deploy/deploy.py --env prod
```

### "Unmatched records not logged"
```bash
./check deploy prod              # Check sequence permissions
# If missing: python sql/deploy/deploy.py --env prod --only permissions
```

### "Prod behaves differently than staging"
```bash
./check compare                  # See differences
# Sync: python sql/deploy/deploy.py --env prod
```

### "Upload failed, don't know why"
```bash
./check prod Roku file.csv       # Full diagnostic
```

## What Each Check Does

| Command | What It Checks | When To Use |
|---------|---------------|-------------|
| `./check` | Critical procedures, buckets, permissions, config | After deployment, when something breaks |
| `./check status prod` | Recent uploads, match rates, common issues | Daily monitoring, after uploads |
| `./check deploy prod` | Everything deployed correctly | After deployment, before prod release |
| `./check compare` | Staging vs prod differences | Before prod deployment |
| `./check fix` | All issues + how to fix | When red flags appear |
| `./check prod Roku file.csv` | Why specific upload failed | When Lambda verification fails |

## Traffic Lights

- ✅ **Green** - All good
- ⚠️ **Yellow** - Works but suboptimal (high unmatched rate, etc)
- ❌ **Red** - Broken (missing procedures, failed permissions, etc)

## Emergency Workflow

```bash
# 1. What's broken?
./check prod

# 2. How do I fix it?
./check fix

# 3. Deploy fix
python sql/deploy/deploy.py --env prod

# 4. Verify
./check prod
```

## Pro Tips

- Run `./check` after EVERY deployment
- Run `./check compare` before deploying to prod
- Run `./check status prod` daily to catch issues early
- Add `./check prod` to CI/CD pipeline

## Output Examples

### Healthy System
```
🏥 HEALTH CHECK
🔴 PROD:
  ✅ ANALYZE_AND_PROCESS_VIEWERSHIP_DATA_GENERIC
  ✅ PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC
  ✅ Templates substituted
  ✅ Sequence (unmatched logging)
  ✅ UDF (asset matching)

✅ ALL SYSTEMS OPERATIONAL
```

### Issues Found
```
🏥 HEALTH CHECK
🔴 PROD:
  ❌ PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC
     ⚠️  ASSET MATCHING WILL FAIL
  ❌ Sequence (unmatched logging)

❌ 2 ISSUE(S) FOUND
Run with --fix to see remediation commands
```

### With Fix Commands
```
./check fix

🔧 FIX COMMANDS:
  python sql/deploy/deploy.py --env prod
    → Fixes: prod: PROCESS_VIEWERSHIP_SERIES_ONLY_GENERIC
    → Fixes: prod: PROCESS_VIEWERSHIP_FULL_DATA_GENERIC
```

## That's It

No more:
- ❌ "Which diagnostic script do I run?"
- ❌ "How do I check if X is working?"
- ❌ "Are staging and prod different?"

Just: `./check`
