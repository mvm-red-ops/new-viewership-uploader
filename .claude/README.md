# Claude Code Instructions for new-viewership-uploader

## CRITICAL: Read This First

**Before answering ANY question about this codebase, you MUST:**

1. **Check TROUBLESHOOTING.md** if the user is reporting an issue
2. **Check ARCHITECTURE.md** if you need to understand how something works
3. **Reference these docs in your response** - cite specific sections

These documents contain institutional knowledge that was repeatedly explained and re-discovered. They exist specifically to avoid re-explaining the same concepts.

---

## Common Scenarios & Required Actions

### User mentions: "git push failed" / "permission denied"
→ **Read TROUBLESHOOTING.md** section "Git / Deployment Issues"
→ Load SSH key: `ssh-add ~/.ssh/work_github`

### User mentions: "dates are wrong" / "parsing incorrectly"
→ **Read TROUBLESHOOTING.md** section "Date Parsing Issues"
→ **Read ARCHITECTURE.md** section "Date Handling System"
→ Check if `detect_date_format` is committed and deployed

### User mentions: "deal_parent is NULL/empty"
→ **Read TROUBLESHOOTING.md** section "Data Pipeline Issues"
→ **Read ARCHITECTURE.md** section "Deal Parent Matching"
→ Check NOSEY_PROD, NOT UPLOAD_DB_PROD

### User mentions: "column not showing up in final table"
→ **Read TROUBLESHOOTING.md** section "Snowflake Stored Procedures"
→ Check `move_data_to_final_table_dynamic_generic` procedure

### User asks: "how does the system work?"
→ **Read ARCHITECTURE.md** thoroughly
→ Reference specific sections in your answer

---

## File Locations Reference

### Documentation (READ THESE)
- `TROUBLESHOOTING.md` - Issue solutions with exact steps
- `ARCHITECTURE.md` - System design and data flow
- `README.md` - Project overview
- `sql/DEPLOYMENT_GUIDE.md` - SQL deployment instructions

### Key Source Files
- `app.py` - Streamlit UI, date auto-detection (lines 2636-2649)
- `src/transformations.py` - Date detection (lines 187-328)
- `src/snowflake_utils.py` - Database operations
- `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` - All stored procedures

### Configuration
- `.streamlit/secrets.toml` - Credentials
- `sql/deploy/config.yaml` - Database name mappings
- `~/.ssh/work_github` - SSH key for git push

---

## Standard Operating Procedures

### When User Reports an Issue

1. **Search relevant documentation first**
   ```bash
   grep -i "keyword" TROUBLESHOOTING.md ARCHITECTURE.md
   ```

2. **If found in docs**: Reference the specific section
   ```
   "According to TROUBLESHOOTING.md (Git / Deployment Issues section),
   you need to load the SSH key: ssh-add ~/.ssh/work_github"
   ```

3. **If not found in docs**: Solve the issue, then ASK if docs should be updated
   ```
   "I've solved this issue. Should I add this solution to TROUBLESHOOTING.md
   so we don't have to rediscover it next time?"
   ```

### When Making Code Changes

1. **Check if docs need updating** - If you modify:
   - Date handling → Update ARCHITECTURE.md "Date Handling System"
   - Database flow → Update ARCHITECTURE.md "Data Flow Overview"
   - Deployment process → Update TROUBLESHOOTING.md

2. **Commit docs with code changes** - Not separately

3. **Update line numbers** if they change significantly

### When User Says "You already built this"

1. **Search git history**
   ```bash
   git log --all --oneline --grep="keyword"
   ```

2. **Check for uncommitted changes**
   ```bash
   git status
   git diff <file>
   ```

3. **Check documentation** - It might be documented but not deployed

---

## Git Workflow

### SSH Key Management
```bash
# ALWAYS do this before git push
ssh-add ~/.ssh/work_github

# Verify it worked
ssh-add -l | grep work_github
```

### Commit Message Format
```
Brief description (imperative: "Add X", not "Added X")

Problem: What issue this solves
Root cause: Why it was happening
Solution: What you changed

Files changed:
- file1: what changed
- file2: what changed

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

### Before Pushing
- Check uncommitted changes: `git status`
- Check if similar work exists: `git log --oneline -20`
- Load SSH key: `ssh-add ~/.ssh/work_github`

---

## Database Investigation Protocol

### Three-Database System (MEMORIZE THIS)

```
UPLOAD_DB_PROD → NOSEY_PROD → ASSETS
   (landing)      (processing)   (final)
   deal_parent:   deal_parent:   deal_parent:
   NULL           GETS SET       CARRIES OVER
```

### When Checking Data
```sql
-- ❌ WRONG - Landing zone, deal_parent always NULL
SELECT * FROM UPLOAD_DB_PROD.public.platform_viewership

-- ✓ CORRECT - Check processing database
SELECT * FROM NOSEY_PROD.public.platform_viewership

-- ✓ ALSO CORRECT - Final destination
SELECT * FROM ASSETS.public.EPISODE_DETAILS
```

### Quick Diagnostic Template
```python
from src.snowflake_utils import SnowflakeConnection
conn = SnowflakeConnection()
conn.cursor.execute("YOUR QUERY")
results = conn.cursor.fetchall()
print(results)
conn.close()
```

---

## Deployment Checklist

### SQL Procedure Changes
1. Edit: `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql`
2. Generate: `cd sql && ./deploy.sh prod DEPLOY_ALL_GENERIC_PROCEDURES.sql`
3. Review: `cat sql/generated/prod_DEPLOY_ALL_GENERIC_PROCEDURES.sql`
4. Deploy: Use `--execute` flag OR Python deployment script (see TROUBLESHOOTING.md)

### Python Code Changes
1. Make changes locally
2. Test with sample data
3. Commit with descriptive message
4. Load SSH key: `ssh-add ~/.ssh/work_github`
5. Push: `git push origin main`
6. Restart Streamlit app if needed

---

## When to Update Documentation

### Add to TROUBLESHOOTING.md when:
- User reports an error you had to debug
- Solution involves multiple steps
- Issue has come up before (even once)
- Involves external tools (git, SSH, Snowflake CLI)

### Add to ARCHITECTURE.md when:
- Adding new database tables
- Changing data flow
- Adding new phases or procedures
- Modifying core logic (date handling, matching, etc.)

### Update existing docs when:
- Line numbers change significantly
- File paths change
- Process changes
- Better solution found

---

## Anti-Patterns to Avoid

❌ **DON'T**: Explain date parsing from scratch without checking ARCHITECTURE.md
✅ **DO**: "See ARCHITECTURE.md 'Date Handling System' section"

❌ **DON'T**: Tell user to check UPLOAD_DB_PROD for deal_parent
✅ **DO**: "Check NOSEY_PROD - see ARCHITECTURE.md 'Three Database System'"

❌ **DON'T**: Rediscover SSH key location every time
✅ **DO**: "Load SSH key per TROUBLESHOOTING.md: ssh-add ~/.ssh/work_github"

❌ **DON'T**: Make changes without checking git status
✅ **DO**: Always check for uncommitted changes first

❌ **DON'T**: Leave good solutions undocumented
✅ **DO**: Ask "Should I add this to TROUBLESHOOTING.md?"

---

## Remember

**These docs exist because this exact situation happened multiple times:**
1. User explains problem
2. You solve it
3. Next conversation, you don't remember the solution
4. User has to explain again
5. Repeat forever

**Break the cycle:**
- Read the docs first
- Reference them in answers
- Update them when they're wrong/incomplete
- Treat them as your persistent memory

The user shouldn't be your documentation. The documentation should be your documentation.
