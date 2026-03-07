# Compliance Duplicates Bug - Root Cause Analysis

## Problem Statement

FRS/David Geraghty PDF (27 Feb 2026) shows 4 duplicate compliance violations:
- All show filing date: 27 Feb 2026 (correct)
- But show trade date: 18 Feb 2026 (WRONG - should be 25 Feb 2026)
- The actual PDF has the correct date: 25 Feb 2026

Result: Same violation appears 4 times with wrong date.

---

## Root Cause

### Scenario 1: asxinsider.com.au API is returning wrong dates
If asxinsider API returns:
```json
{
  "symbol": "FRS",
  "director_name": "David Geraghty",
  "date_of_change": "2026-02-18",  // WRONG - API has incorrect date
  "dateReadable": "2026-02-27",    // Correct filing date
  "quantity": 50000,
  ...
}
```

And the database already has (from PDF parser):
```sql
SELECT * FROM trades
WHERE ticker='FRS'
  AND director_name='David Geraghty'
  AND date_of_trade='2026-02-25'  -- CORRECT from PDF
  AND quantity=50000
```

**The sync deduplication check looks for:**
```python
async def find_existing_trade(
    session, company_id, director_id, trade_date, quantity
):
    # Key: (company_id, director_id, date_of_trade, quantity)
    SELECT Trade WHERE
      company_id=X
      AND director_id=Y
      AND date_of_trade='2026-02-18'  # Searching for WRONG date
      AND quantity=50000
```

**Result:** No match found (because PDF has 25 Feb, API says 18 Feb)
- Sync inserts new record: `(FRS, Geraghty, 18 Feb, 50000, asxinsider_gpt)`
- DB now has both:
  - `(FRS, Geraghty, 25 Feb, 50000, pdf_parser)` ← CORRECT from PDF
  - `(FRS, Geraghty, 18 Feb, 50000, asxinsider_gpt)` ← WRONG from API

**If this happened 4 times** (asxinsider returned 4 copies of the same transaction with wrong date), we'd get 4 violations with wrong date.

---

### Why This Happens

1. **asxinsider.com.au data quality issue** — Their API might be extracting dates incorrectly or returning duplicate/stale records
2. **Sync deduplication is date-sensitive** — It uses date_of_trade as part of the key, so date mismatches create "new" records
3. **Compliance endpoint doesn't deduplicate** — Returns ALL violation trades without grouping

---

### Why It's "Not Always Wrong"

- **Most trades match correctly** — Usually dates align between PDF parser and asxinsider API
- **No duplicates in normal cases** — When dates match, dedup works and prevents inserts
- **Only breaks when dates differ** — Happens when asxinsider API has wrong date for a transaction
- **FRS/Geraghty PDF is an outlier** — This specific PDF has a date extraction error in the asxinsider data

---

## The Fix

We need to **deduplicate at the compliance violation level**, not just at the trade insertion level.

### Current Behavior
```sql
SELECT DISTINCT trade_id FROM violations
-- Returns 4 separate trade IDs (all for same transaction, different dates)
```

### Fixed Behavior
```sql
SELECT DISTINCT ON (ticker, director_name, date_lodged, date_of_trade)
-- Returns 1 violation (deduplicated by who, when filed, and the correct trade date)
-- If there are multiple with different dates, pick the correct one (pdf_parser source)
```

### Implementation Strategy

**Option A: Deduplicate in the API response** (quick fix, for this release)
- Group violations by `(ticker, director_id, date_lodged, date_of_trade)`
- When duplicates exist, keep the one with source='pdf_parser' (more reliable)
- Return deduplicated list

**Option B: Deduplicate in the database** (comprehensive fix)
- Add dedup check in sync_from_asxinsider.py to prevent inserting date-mismatched records
- Check existing records not just on (ticker, director, date, qty) but also verify date makes sense
- When dates don't match and we have both pdf_parser and asxinsider_gpt, keep pdf_parser

**Option C: Deduplicate in both places** (most robust)
- Fix at sync level: better matching logic
- Fix at API level: return deduplicated violations

---

## Recommended Fix

1. **Immediate (for this release):** Deduplicate in the compliance API endpoint
   - Prevents duplicate violations from showing to users
   - Easy to implement and test
   - Doesn't require data cleanup

2. **Follow-up (next release):** Improve sync dedup logic
   - Detect when asxinsider date differs from pdf_parser date by > 1 week
   - Skip inserting if pdf_parser record already exists with reasonable date
   - Trust pdf_parser over asxinsider for date accuracy

3. **Monitoring:** Add logging to detect when dates don't match
   - Log when sync finds asxinsider trade with different date than expected
   - Flag for manual review

---

## Testing the Fix Locally

```bash
docker-compose exec backend python inspect_compliance.py
```

Shows all FRS/Geraghty trades Feb 20-Mar 5 with duplicates highlighted.

Then after fix:
```bash
curl http://localhost:8000/api/compliance/violations | jq '.[] | select(.ticker=="FRS")'
```

Should show 1 violation for each unique (ticker, director, date_of_trade, date_lodged).

