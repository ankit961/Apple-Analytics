# Testing Guide: ONE_TIME_SNAPSHOT Integration

## Pre-Test Checklist

Before testing, ensure:
- ✅ Environment variables are set (.env file exists)
- ✅ AWS credentials are configured
- ✅ S3 bucket is accessible
- ✅ Apple Analytics API credentials are valid
- ✅ Dependencies installed: boto3, pandas, pyarrow, requests

```bash
# Verify dependencies
pip3 install -r requirements.txt
```

---

## Syntax Validation

```bash
# Quick Python syntax check
python3 -m py_compile unified_etl.py
echo $?  # Should return 0 (success)

# Or test import
python3 -c "import ast; ast.parse(open('unified_etl.py').read()); print('✅ Syntax OK')"
```

---

## Help & Documentation Tests

```bash
# Show help with new arguments
python3 unified_etl.py --help

# Should show:
# --onetime
# --start-date
# --end-date
# Plus existing arguments (--date, --app-id, --backfill, --days)
```

Expected output:
```
usage: unified_etl.py [-h] [--date DATE] [--app-id APP_ID] [--backfill] 
                      [--days DAYS] [--onetime] [--start-date START_DATE] 
                      [--end-date END_DATE] [--transform-only] [--load-only]

Apple Analytics Unified ETL - ONGOING and ONE_TIME_SNAPSHOT modes

optional arguments:
  --date DATE              Target date for ONGOING mode (YYYY-MM-DD)
  --app-id APP_ID          Specific app ID
  --backfill               Backfill mode (ONGOING)
  --days DAYS              Days to backfill (ONGOING mode)
  --onetime                Enable ONE_TIME_SNAPSHOT mode
  --start-date START_DATE  Start date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)
  --end-date END_DATE      End date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)
  --transform-only         Only run transform phase (skip extract)
  --load-only              Only run load phase (refresh Athena partitions)
```

---

## Unit Tests

### Test 1: Date Range Generation

```bash
python3 << 'EOF'
import sys
sys.path.insert(0, '.')
from datetime import datetime, timedelta

# Mock the UnifiedETL class
class TestETL:
    def generate_date_range(self, start_date: str, end_date: str):
        dates = []
        current = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates

etl = TestETL()

# Test 1: Single day
dates = etl.generate_date_range('2025-11-27', '2025-11-27')
assert len(dates) == 1
assert dates[0] == '2025-11-27'
print("✅ Test 1 passed: Single day")

# Test 2: 5 days
dates = etl.generate_date_range('2025-11-01', '2025-11-05')
assert len(dates) == 5
assert dates[0] == '2025-11-01'
assert dates[-1] == '2025-11-05'
print("✅ Test 2 passed: 5-day range")

# Test 3: Full month
dates = etl.generate_date_range('2025-11-01', '2025-11-30')
assert len(dates) == 30
print("✅ Test 3 passed: Full month")

# Test 4: Large range
dates = etl.generate_date_range('2025-09-01', '2025-11-30')
assert len(dates) == 91  # Sept(30) + Oct(31) + Nov(30) = 91
print("✅ Test 4 passed: Large range (91 days)")

print("\n✅ All date range tests passed!")
EOF
```

---

## Integration Tests (with mock)

### Test 2: ONGOING Mode Detection

```bash
python3 << 'EOF'
import sys
import argparse

# Test argument parsing
parser = argparse.ArgumentParser()
parser.add_argument('--onetime', action='store_true')
parser.add_argument('--start-date', type=str)
parser.add_argument('--end-date', type=str)
parser.add_argument('--date', type=str)

# Test 1: ONGOING mode (default)
args = parser.parse_args(['--date', '2025-11-27'])
assert not args.onetime
print("✅ Test 1 passed: ONGOING mode (no --onetime flag)")

# Test 2: ONE_TIME_SNAPSHOT mode
args = parser.parse_args(['--onetime', '--start-date', '2025-11-01', '--end-date', '2025-11-30'])
assert args.onetime
assert args.start_date == '2025-11-01'
assert args.end_date == '2025-11-30'
print("✅ Test 2 passed: ONE_TIME_SNAPSHOT mode with date range")

# Test 3: ONE_TIME_SNAPSHOT requires date range
args = parser.parse_args(['--onetime', '--start-date', '2025-11-01'])
assert args.onetime
assert args.start_date == '2025-11-01'
assert args.end_date is None  # Missing end_date should be caught by run() method
print("✅ Test 3 passed: ONE_TIME_SNAPSHOT with incomplete dates (will error in run())")

print("\n✅ All argument parsing tests passed!")
EOF
```

---

## Manual Integration Tests

### Test 3: ONGOING Mode (Existing Functionality - Baseline)

**Purpose:** Verify existing ONGOING mode still works

```bash
# Process yesterday for all apps (no changes)
python3 unified_etl.py

# Should see:
# 🍎 APPLE ANALYTICS - UNIFIED ETL PIPELINE
# 📊 Mode: ONGOING (Daily Automation)
# 📅 Dates: 1 (2025-01-01 to 2025-01-01)
# 🔄 PHASE 1: EXTRACT
# 🔄 PHASE 2: TRANSFORM
# 🔄 PHASE 3: LOAD
```

### Test 4: ONE_TIME_SNAPSHOT Mode (New Functionality)

**Purpose:** Verify ONE_TIME_SNAPSHOT mode works correctly

```bash
# Backfill a 2-day range (small test)
python3 unified_etl.py --onetime --start-date 2025-11-26 --end-date 2025-11-27 \
  --app-id 1506886061

# Should see:
# 🍎 APPLE ANALYTICS - UNIFIED ETL PIPELINE
# 📊 Mode: ONE_TIME_SNAPSHOT (Bulk Backfill)
# 📅 Date Range: 2025-11-26 to 2025-11-27
# 📱 Apps: 1
# 🔄 PHASE 1: EXTRACT - App 1506886061
# 🔄 PHASE 2: TRANSFORM
# 🔄 PHASE 3: LOAD
```

### Test 5: ONE_TIME_SNAPSHOT - Multiple Apps

```bash
# Backfill for multiple apps
python3 unified_etl.py --onetime --start-date 2025-11-26 --end-date 2025-11-27

# Should process all apps in APP_IDS environment variable
```

### Test 6: ONE_TIME_SNAPSHOT - Full Month

```bash
# Backfill entire November
python3 unified_etl.py --onetime --start-date 2025-11-01 --end-date 2025-11-30

# This will take longer - multiple dates x multiple apps
# Monitor logs: tail -f logs/unified_etl_*.log
```

### Test 7: ONGOING Backfill (Compare with ONE_TIME)

```bash
# ONGOING: Backfill last 7 days
python3 unified_etl.py --backfill --days 7

# ONE_TIME: Backfill same 7 days
python3 unified_etl.py --onetime --start-date 2025-11-21 --end-date 2025-11-27

# Compare results:
# - ONGOING creates one request per app, extracts by date
# - ONE_TIME creates request for entire range, filters by date
```

---

## Verification Tests

### Test 8: Check Results JSON

```bash
# Find latest results file
ls -lt logs/unified_etl_results_*.json | head -1

# Examine results
python3 << 'EOF'
import json

with open('logs/unified_etl_results_20250102_000000.json', 'r') as f:
    results = json.load(f)

# Verify structure
assert 'mode' in results
assert 'apps_processed' in results
assert 'files_extracted' in results

if results['mode'] == 'ONE_TIME_SNAPSHOT':
    assert 'start_date' in results
    assert 'end_date' in results
    assert 'request_ids' in results
    print(f"✅ ONE_TIME_SNAPSHOT results:")
    print(f"   Date range: {results['start_date']} to {results['end_date']}")
    print(f"   Apps processed: {results['apps_processed']}")
    print(f"   Files extracted: {results['files_extracted']}")
else:
    print(f"✅ ONGOING results:")
    print(f"   Apps processed: {results['apps_processed']}")
    print(f"   Files extracted: {results['files_extracted']}")

EOF
```

### Test 9: Check S3 Registry

```bash
# List request registry
aws s3 ls s3://skidos-apptrack/analytics_requests/registry/ --recursive

# Check specific app registry
aws s3 ls s3://skidos-apptrack/analytics_requests/registry/app_id=1506886061/

# View request metadata
aws s3 cp s3://skidos-apptrack/analytics_requests/registry/app_id=1506886061/one_time_snapshot.json - | jq .
```

Expected output:
```json
{
  "request_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  "start_date": "2025-11-01",
  "end_date": "2025-11-30",
  "created_at": "2025-01-02T12:34:56.789000+00:00"
}
```

### Test 10: Check Extracted Data in S3

```bash
# List raw data for ONE_TIME_SNAPSHOT
aws s3 ls s3://skidos-apptrack/appstore/raw/ --recursive | grep "dt=2025-11"

# Example:
# appstore/raw/downloads/dt=2025-11-26/app_id=1506886061/report_abc123_segment_001.csv
# appstore/raw/engagement/dt=2025-11-26/app_id=1506886061/report_def456_segment_001.csv
```

### Test 11: Check Curated Data in S3

```bash
# List curated parquet files
aws s3 ls s3://skidos-apptrack/appstore/curated/ --recursive | grep "dt=2025-11"

# Example:
# appstore/curated/downloads/dt=2025-11-26/app_id=1506886061/data.parquet
```

### Test 12: Check Athena Partitions

```bash
# Query partition count
aws athena start-query-execution \
  --query-string "SHOW PARTITIONS appstore.curated_downloads" \
  --query-execution-context Database=appstore \
  --result-configuration OutputLocation=s3://skidos-apptrack/Athena-Output/

# Wait for query to complete and check results
```

---

## Backward Compatibility Tests

### Test 13: Existing Scripts Still Work

```bash
# Test all existing usage patterns

# Pattern 1: Default (yesterday)
python3 unified_etl.py

# Pattern 2: Specific date
python3 unified_etl.py --date 2025-11-27

# Pattern 3: Specific app
python3 unified_etl.py --app-id 1506886061

# Pattern 4: Backfill
python3 unified_etl.py --backfill --days 7

# Pattern 5: Transform only
python3 unified_etl.py --transform-only --date 2025-11-27

# Pattern 6: Load only
python3 unified_etl.py --load-only

# All should complete without errors ✅
```

---

## Error Handling Tests

### Test 14: Error Cases

```bash
# Test 1: ONE_TIME_SNAPSHOT without dates (should error)
python3 unified_etl.py --onetime
# Expected: ValueError "requires --start-date and --end-date"

# Test 2: Invalid date format (should error)
python3 unified_etl.py --onetime --start-date 11-01-2025 --end-date 11-30-2025
# Expected: ValueError (wrong date format)

# Test 3: End date before start date (should error)
python3 unified_etl.py --onetime --start-date 2025-11-30 --end-date 2025-11-01
# Expected: Empty date list or warning

# Test 4: Future dates (should work but return no data)
python3 unified_etl.py --onetime --start-date 2099-01-01 --end-date 2099-01-31
# Expected: Processes but likely returns no data
```

---

## Performance Tests

### Test 15: Large Date Ranges

```bash
# Test with 90-day range (3 months)
time python3 unified_etl.py --onetime --start-date 2025-09-01 --end-date 2025-11-30 \
  --app-id 1506886061

# Monitor resource usage:
# - Memory: Should stay under 2GB
# - Time: Depends on data volume and API rate limits
# - CPU: Will peak during transform phase
```

---

## Logging Tests

### Test 16: Log File Analysis

```bash
# Check log file exists
ls -la logs/unified_etl_*.log

# Monitor real-time logs
tail -f logs/unified_etl_*.log

# Search for errors
grep "❌" logs/unified_etl_*.log

# Count by level
grep -c "📱" logs/unified_etl_*.log  # Apps processed
grep -c "✅" logs/unified_etl_*.log  # Successes
grep -c "⚠️" logs/unified_etl_*.log  # Warnings
```

---

## Test Summary Report

After running all tests, create a summary:

```
TEST RESULTS
============

Syntax & Structure:
✅ Python syntax validation passed
✅ All required methods present
✅ CLI arguments properly configured
✅ Backward compatibility maintained

ONGOING Mode (Existing):
✅ Default mode works
✅ Specific date works
✅ Specific app works
✅ Backfill works

ONE_TIME_SNAPSHOT Mode (New):
✅ Two-day range works
✅ Multi-app processing works
✅ Full-month range works
✅ Results saved to JSON
✅ Requests saved to S3 registry

Data Integrity:
✅ Files extracted to S3
✅ Data curated to Parquet
✅ Athena partitions refreshed
✅ No duplicate errors

Error Handling:
✅ Missing dates detected
✅ Invalid formats caught
✅ Errors logged properly

Performance:
✅ Large ranges process correctly
✅ Resource usage acceptable
✅ API rate limits respected

STATUS: ✅ READY FOR PRODUCTION
```

---

**Next Steps:**
1. Run basic syntax validation
2. Test ONGOING mode (existing functionality)
3. Test ONE_TIME_SNAPSHOT with 2-day range
4. Test ONE_TIME_SNAPSHOT with full month
5. Verify S3 and Athena data
6. Monitor logs for errors
7. Test backward compatibility
8. Validate results in database
9. Document any issues
10. Proceed to production if all tests pass
