# System Architecture & Key Concepts

This document explains how the system works so you don't have to rediscover it every time.

## Table of Contents
- [Data Flow Overview](#data-flow-overview)
- [Database Architecture](#database-architecture)
- [Date Handling System](#date-handling-system)
- [Deal Parent Matching](#deal-parent-matching)
- [Key Files & Their Roles](#key-files--their-roles)

---

## Data Flow Overview

### Streamlit Upload Path

```
1. User uploads CSV via Streamlit UI (app.py)
   ↓
2. Column mapping & transformations applied (app.py lines 2580-2708)
   - Date auto-detection happens HERE (lines 2636-2649)
   - Creates standardized dataframe
   ↓
3. Data inserted into UPLOAD_DB_PROD.public.platform_viewership
   - Via src/snowflake_utils.py load_to_platform_viewership()
   ↓
4. Lambda invoked (lambdas/register-data-lambda/)
   ↓
5. Lambda copies data to NOSEY_PROD.public.platform_viewership
   ↓
6. Lambda runs Phase 2: Content References
   - set_deal_parent_generic (deal matching)
   - set_channel_generic (fallback)
   - set_territory_generic (normalization)
   - Asset matching (ref_id, content_provider, etc.)
   ↓
7. Lambda runs Phase 3: Final Insert
   - Validates data
   - Inserts into ASSETS.public.EPISODE_DETAILS
   ↓
8. Done! Data in final table with all references
```

### Key Insight: Three Databases

| Database | Purpose | deal_parent Status |
|----------|---------|-------------------|
| UPLOAD_DB_PROD | Landing zone | Always NULL (just uploaded) |
| NOSEY_PROD | Processing/staging | Gets SET during Phase 2 |
| ASSETS | Final destination | Carries over from NOSEY_PROD |

**NEVER check UPLOAD_DB_PROD for deal_parent!** It's always NULL there. Check NOSEY_PROD or ASSETS.

---

## Date Handling System

### The Problem

Different platforms send dates in different formats:
- Philo: `06-07-2025` (DD-MM-YYYY)
- YouTube: `07/06/2025` (MM/DD/YYYY)
- Roku: `2025-07-06` (YYYY-MM-DD)

pandas `to_datetime()` defaults to MM-DD-YYYY, causing misinterpretation.

### The Solution: Auto-Detection

**Location:** `src/transformations.py` lines 187-328

**Auto-Application:** `app.py` lines 2636-2649

**Result:** All dates converted to `YYYY-MM-DD` before database insert.

---

## Deal Parent Matching

**Stored Procedure:** `set_deal_parent_generic` (sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql line 562)

Matches on: platform, domain, partner_name, channel_name, territory (all case-insensitive except platform)

**Multi-Territory Support:** As of Dec 8, 2025, the `territory` field in `platform_viewership` is stored as an ARRAY to support multiple territories per record. Template configurations in `COLUMN_MAPPING_CONFIGS` also use ARRAY type for the `territories` column.

**Common Issue:** Check NOSEY_PROD, not UPLOAD_DB_PROD for deal_parent!

---

## Key Files

- `app.py` - Main UI, date auto-detection at lines 2636-2649
- `src/transformations.py` - detect_date_format() at lines 187-328
- `src/snowflake_utils.py` - Database operations
- `sql/templates/DEPLOY_ALL_GENERIC_PROCEDURES.sql` - All stored procedures

See TROUBLESHOOTING.md for specific issues.
