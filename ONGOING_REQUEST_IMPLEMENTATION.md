# Apple ETL Pipeline - ONGOING Request Implementation

## Summary of Changes (2025-11-27)

### Problem Solved
The ETL pipeline was using `ONE_TIME_SNAPSHOT` requests which:
1. Caused 409 conflicts when requests already existed
2. Were not suitable for daily automated extraction
3. Required manual intervention to handle conflicts

### Solution Implemented
Changed the pipeline to use `ONGOING` requests which:
1. ✅ Can be reused indefinitely (no 409 conflicts)
2. ✅ Are stored in S3 registry for fast lookup
3. ✅ Automatically find existing requests via Apple API
4. ✅ Are the correct choice for daily ETL operations

---

## Files Modified

### 1. `src/extract/apple_analytics_client.py`
Added new methods for ONGOING request handling:

```python
# New methods added:
- create_or_reuse_ongoing_request(app_id) -> str
- _find_existing_ongoing_request(app_id) -> str  
- _verify_request_exists(request_id) -> bool
```

The `create_or_reuse_ongoing_request()` method:
1. Checks S3 registry first (fast)
2. Falls back to Apple API query
3. Creates new request only if none exists
4. Saves request ID to registry for future use

### 2. `src/extract/focused_data_extractor.py`
Updated to support both ONGOING and ONE_TIME_SNAPSHOT:

```python
def extract_app_business_data(self, app_id: str, use_ongoing: bool = True):
    # Now defaults to ONGOING for daily ETL
```

### 3. `daily_etl.py` (NEW)
Created new production-ready daily ETL script:

```bash
# Usage:
python3 daily_etl.py                      # Extract yesterday's data for all apps
python3 daily_etl.py --app-id 1506886061  # Extract for specific app
python3 daily_etl.py --date 2025-11-26    # Extract for specific date
```

Features:
- Uses ONGOING requests exclusively
- Handles all apps from APP_IDS environment variable
- Proper error handling and logging
- Saves results to JSON file

### 4. `run_daily_etl.sh`
Updated to use the new daily_etl.py script:
- Simplified workflow
- Uses virtual environment properly
- Passes through command-line arguments

---

## S3 Registry Structure

```
s3://skidos-apptrack/analytics_requests/registry/
  └── app_id={APP_ID}/
      ├── ongoing.json        # ONGOING request ID (preferred)
      └── one_time_snapshot.json  # ONE_TIME request ID (legacy)
```

Example registry entry:
```json
{
  "app_id": "1506886061",
  "access_type": "ONGOING",
  "request_id": "504fb4a0-4b4d-43e3-b50b-7d359d910924",
  "created_at": "2025-11-27T18:28:35.782142+00:00"
}
```

---

## Verification

Tested with app 1506886061:
1. ✅ Found existing ONGOING request via Apple API
2. ✅ Saved to S3 registry
3. ✅ Subsequent runs reuse from registry (no API call needed)
4. ✅ Downloaded 9 files to S3 for 2025-11-26

---

## Workspace Cleanup

Moved unnecessary files to archive:
- `archive/old_docs_2025_11/` - Old documentation files
- `archive/old_scripts_2025_11/` - Old Python and shell scripts
- `archive/old_results_2025_11/` - Old result files

Root directory now contains only essential files:
- Core project directories (Apple-Analytics, airflow, dashboard_api, etc.)
- Configuration files (.env, requirements.txt)
- Documentation (README.md, QUICK_START.md)

---

## Daily ETL Usage

### Manual Run
```bash
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics
source ../.venv/bin/activate
python3 daily_etl.py
```

### Via Shell Script
```bash
./run_daily_etl.sh
# or with specific options:
./run_daily_etl.sh --app-id 1506886061 --date 2025-11-26
```

### Cron Job (for automation)
```cron
# Run daily at 4 PM UTC (after Apple makes data available)
0 16 * * * cd /path/to/Apple-Analytics && ./run_daily_etl.sh >> /var/log/apple_etl.log 2>&1
```

---

## Key Differences: ONGOING vs ONE_TIME_SNAPSHOT

| Feature | ONGOING | ONE_TIME_SNAPSHOT |
|---------|---------|-------------------|
| Reusable | ✅ Yes, indefinitely | ❌ No, single use |
| 409 Conflicts | ❌ No (can query existing) | ✅ Yes (can't find existing) |
| Daily ETL | ✅ Recommended | ❌ Not suitable |
| Backfills | ⚠️ Possible | ✅ Designed for this |
| Data Freshness | Daily updates | Point-in-time |
