# Quick Reference: ONE_TIME_SNAPSHOT Integration

## What Changed?

✅ `unified_etl.py` now supports **two request types:**
1. **ONGOING** (existing) - For daily automation
2. **ONE_TIME_SNAPSHOT** (new) - For backfilling historical data

## New CLI Commands

```bash
# ONE_TIME_SNAPSHOT: Backfill November 2025
python3 unified_etl.py --onetime --start-date 2025-11-01 --end-date 2025-11-30

# ONE_TIME_SNAPSHOT: Backfill specific app
python3 unified_etl.py --onetime --start-date 2025-11-01 --end-date 2025-11-30 \
  --app-id 1506886061

# ONGOING: Daily mode (unchanged)
python3 unified_etl.py

# ONGOING: Backfill 30 days
python3 unified_etl.py --backfill --days 30
```

## New Methods in UnifiedETL Class

1. **`generate_date_range(start, end)`** → List[str]
   - Generates dates between start and end

2. **`create_onetime_request_for_range(app_id, start, end)`** → str
   - Creates/reuses ONE_TIME request for date range

3. **`_validate_request_is_available(request_id)`** → bool
   - Checks if request is still valid

4. **`_extract_onetime_data(app_id, request_id, date)`** → Dict
   - Extracts data from ONE_TIME request for specific date

5. **`run(..., onetime, start_date, end_date)`** (UPDATED)
   - Now supports both request types

## Key Features

✅ **Backward compatible** - All existing scripts work unchanged  
✅ **Deduplication** - ONE_TIME_SNAPSHOT handles overlapping data  
✅ **Request reuse** - Saves requests to S3 registry  
✅ **Error handling** - Graceful handling of missing/expired requests  
✅ **Unified pipeline** - Single script for both modes  

## Testing

```bash
# Test with small date range (2 days)
python3 unified_etl.py --onetime --start-date 2025-11-26 --end-date 2025-11-27 \
  --app-id 1506886061

# Check logs
tail -f logs/unified_etl_*.log

# View results
ls -la logs/unified_etl_results_*.json
```

## Mode Selection Logic

```
if --onetime is provided:
  → Use ONE_TIME_SNAPSHOT mode with date range
else:
  → Use ONGOING mode (default)
```

## File Structure

```
unified_etl.py
├── UnifiedETL class
│   ├── generate_date_range()           [NEW]
│   ├── _validate_request_is_available() [NEW]
│   ├── create_onetime_request_for_range() [NEW]
│   ├── _extract_onetime_data()         [NEW]
│   ├── extract_app_data()              [unchanged]
│   ├── transform_to_parquet()          [unchanged]
│   └── run()                           [UPDATED]
└── main()                              [UPDATED]
```

## One-Line Summary

**Before:** unified_etl.py only supported daily ONGOING requests  
**After:** unified_etl.py supports both ONGOING (daily) and ONE_TIME_SNAPSHOT (backfill) modes in a single unified script

---

**Status:** ✅ Ready to use
