# Data Freshness Monitoring - Complete Guide

## 🎯 Overview

The data freshness monitoring system answers the critical question: **"Did all apps get fresh data today?"**

Instead of just checking API responses, we verify that:
1. ✅ New RAW files were created in S3
2. ✅ New CURATED parquet files were generated
3. ✅ Registries are being used and staying fresh

---

## 📊 Monitoring Strategy

### 3-Layer Verification System

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: ETL Logs (Real-time)                                   │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│ • Check logs during pipeline run                                │
│ • See which apps succeeded/failed                               │
│ • Identify errors as they happen                                │
│ • Search for specific patterns                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: Data Freshness Check (Post-run validation)             │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│ • Verify files were created in S3                               │
│ • Check both RAW and CURATED data                               │
│ • Confirm data is for correct date                              │
│ • Identify missing data types                                   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Multi-Day Trends (Long-term health)                    │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ │
│ • Track success rate over time                                  │
│ • Spot degradation patterns                                     │
│ • Compare week-over-week                                        │
│ • Registry aging analysis                                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 How to Use the Monitor

### Method 1: Check Yesterday's Data (Daily Routine)

```bash
cd /data/apple-analytics
python3 monitor_data_freshness.py
```

**When to run:** Every morning at ~10:00 UTC (after cron completes)

**Output:**
```
================================================================================
📊 DATA FRESHNESS REPORT - 2026-01-16
================================================================================
📅 Processing Date: 2026-01-16
📈 Metric Date: 2026-01-15
📱 Total Apps: 92

================================================================================
🔍 DATA FRESHNESS SUMMARY
================================================================================
✅ Apps with fresh RAW data:     88/92 (95.7%)
✅ Apps with fresh CURATED data: 85/92 (92.4%)

⚠️  Apps MISSING data: 7
   - 1506886061 (registry age: 48 days, missing: purchases)
   - 6443744460 (registry age: 48 days, missing: downloads, sessions)

================================================================================
📊 DATA TYPE BREAKDOWN
================================================================================
✅ downloads   : 85/92 apps ( 92.4%)
✅ engagement  : 88/92 apps ( 95.7%)
✅ sessions    : 87/92 apps ( 94.6%)
✅ installs    : 90/92 apps ( 97.8%)
⚠️  purchases   : 72/92 apps ( 78.3%)

================================================================================
📖 REGISTRY STATUS
================================================================================
Average registry age: 48.3 days
Oldest registry:      48 days
Newest registry:      47 days

Age distribution:
  0-30d: 0 apps
  31-60d: 92 apps
  61-90d: 0 apps
  90+d: 0 apps
================================================================================

📄 Report saved to: logs/data_freshness_2026-01-16.json
```

---

### Method 2: Check Specific Date

```bash
# Check data from Jan 15
python3 monitor_data_freshness.py --date 2026-01-15

# Check data from last week
python3 monitor_data_freshness.py --date 2026-01-09
```

**When to use:** 
- Investigating a specific day's issues
- Backfilling data verification
- Comparing different dates

---

### Method 3: Multi-Day Trend Analysis

```bash
# Check last 7 days
python3 monitor_data_freshness.py --days 7

# Check last 30 days
python3 monitor_data_freshness.py --days 30
```

**Output:**
```
================================================================================
📈 7-DAY DATA FRESHNESS TREND
================================================================================
✅ 2026-01-16: 85/92 apps (92.4%)
✅ 2026-01-15: 33/92 apps (35.9%)
✅ 2026-01-14: 66/92 apps (71.7%)
✅ 2026-01-13: 74/92 apps (80.4%)
⚠️  2026-01-12: 45/92 apps (48.9%)
✅ 2026-01-11: 29/92 apps (31.5%)
✅ 2026-01-10: 82/92 apps (89.1%)
================================================================================
```

**When to use:**
- Weekly health check
- Identify degradation patterns
- Before/after deployment comparison
- Monthly reports

---

## 📋 What the Monitor Checks

### 1. RAW Data Files (S3)

**Location:** `s3://skidos-apptrack/appstore/raw/{data_type}/dt={processing_date}/app_id={app_id}/`

**Checks:**
- ✅ Files exist for processing date
- ✅ Count of CSV files (should be 5 per data type)
- ✅ File sizes (detect empty files)
- ✅ Last modified timestamp

**Example:**
```
s3://skidos-apptrack/appstore/raw/downloads/dt=2026-01-16/app_id=1506886061/
  ├── downloads_2026-01-12.csv (1.2 MB)
  ├── downloads_2026-01-13.csv (1.1 MB)
  ├── downloads_2026-01-14.csv (1.3 MB)
  ├── downloads_2026-01-15.csv (1.2 MB)
  └── downloads_2026-01-16.csv (1.1 MB)
```

---

### 2. CURATED Data Files (S3)

**Location:** `s3://skidos-apptrack/appstore/curated/{data_type}/dt={metric_date}/app_id={app_id}/data.parquet`

**Checks:**
- ✅ Parquet file exists
- ✅ File size (detect empty files)
- ✅ Last modified timestamp
- ✅ All 5 data types present

**Example:**
```
s3://skidos-apptrack/appstore/curated/downloads/dt=2026-01-15/app_id=1506886061/data.parquet (850 KB)
s3://skidos-apptrack/appstore/curated/engagement/dt=2026-01-15/app_id=1506886061/data.parquet (620 KB)
s3://skidos-apptrack/appstore/curated/sessions/dt=2026-01-15/app_id=1506886061/data.parquet (540 KB)
s3://skidos-apptrack/appstore/curated/installs/dt=2026-01-15/app_id=1506886061/data.parquet (320 KB)
s3://skidos-apptrack/appstore/curated/purchases/dt=2026-01-15/app_id=1506886061/data.parquet (180 KB)
```

---

### 3. Registry Status

**Location:** `s3://skidos-apptrack/appstore/registry/ongoing_requests/{app_id}.json`

**Checks:**
- ✅ Registry exists
- ✅ Registry age (days since creation)
- ✅ Last verified timestamp
- ✅ Request ID present

**Example Registry:**
```json
{
  "request_id": "aa3b58a1-7654-4321-abcd-ef1234567890",
  "request_type": "ONGOING",
  "app_id": "1506886061",
  "app_name": "Skidos",
  "created_at": "2025-11-28T10:30:45.123456Z",
  "last_verified": "2026-01-16T09:35:12.654321Z"
}
```

---

## 🔍 How to Interpret Results

### ✅ Healthy Pipeline

```
✅ Apps with fresh RAW data:     88/92 (95.7%)
✅ Apps with fresh CURATED data: 85/92 (92.4%)
```

**Indicators:**
- Success rate ≥ 80%
- Most data types > 90% coverage
- Registry ages between 0-60 days
- Few or no missing apps

**Action:** ✅ No action needed, pipeline is healthy

---

### ⚠️ Degraded Performance

```
⚠️  Apps with fresh RAW data:     65/92 (70.7%)
⚠️  Apps with fresh CURATED data: 62/92 (67.4%)
```

**Indicators:**
- Success rate between 50-79%
- Some data types < 80% coverage
- Registry ages approaching 60+ days
- Increasing missing apps

**Action:** 
1. Check ETL logs for errors
2. Review failed apps list
3. Investigate specific data types with low coverage
4. May need intervention soon

---

### ❌ Pipeline Failure

```
❌ Apps with fresh RAW data:     30/92 (32.6%)
❌ Apps with fresh CURATED data: 28/92 (30.4%)
```

**Indicators:**
- Success rate < 50%
- Multiple data types failing
- Many apps missing data
- Registry ages > 60 days

**Action:**
1. **URGENT:** Check ETL logs immediately
2. Review deployment changes
3. Check for API issues (403, 429, 409 errors)
4. May need rollback or hotfix

---

## 📊 Understanding Data Types

### Expected Coverage by Data Type

| Data Type | Expected Coverage | Notes |
|-----------|-------------------|-------|
| **downloads** | 90-95% | Most reliable |
| **engagement** | 90-95% | Most reliable |
| **sessions** | 90-95% | Most reliable |
| **installs** | 85-95% | Slightly lower for mature apps |
| **purchases** | 70-85% | Lower (many apps have no purchases) |

**Why purchases are lower:**
- Many apps are free with no in-app purchases
- Apple only returns data if purchases occurred
- This is expected and normal

---

## 🔔 Integration with Slack

### Automated Alerts (Future Enhancement)

```bash
# Add to crontab after ETL run
35 9 * * * cd /data/apple-analytics && python3 monitor_data_freshness.py > /tmp/freshness.txt && python3 send_to_slack.py /tmp/freshness.txt
```

**Slack message will show:**
```
📊 Apple Analytics - Daily Freshness Report

✅ Success Rate: 85/92 apps (92.4%)

📈 Breakdown:
  • downloads:   85/92 (92.4%) ✅
  • engagement:  88/92 (95.7%) ✅
  • sessions:    87/92 (94.6%) ✅
  • installs:    90/92 (97.8%) ✅
  • purchases:   72/92 (78.3%) ⚠️

📖 Registry Health:
  • Average age: 48.3 days ✅
  • Oldest: 48 days ✅

⚠️  7 apps missing data:
  • 1506886061 (missing: purchases)
  • 6443744460 (missing: downloads, sessions)
  ...
```

---

## 📁 Saved Reports

### JSON Reports

**Location:** `logs/data_freshness_{date}.json`

**Contents:**
```json
{
  "report_date": "2026-01-16",
  "metric_date": "2026-01-15",
  "total_apps": 92,
  "apps_with_fresh_raw_data": 88,
  "apps_with_fresh_curated_data": 85,
  "apps_missing_data": ["1506886061", "6443744460", ...],
  "data_by_type": {
    "downloads": {"fresh": 85, "missing": 7},
    "engagement": {"fresh": 88, "missing": 4},
    ...
  },
  "details": {
    "1506886061": {
      "app_id": "1506886061",
      "raw_files": 25,
      "curated_files": 4,
      "missing_types": ["purchases"],
      "registry": {
        "request_id": "aa3b58a1...",
        "age_days": 48,
        "created_at": "2025-11-28T10:30:45Z",
        "last_verified": "2026-01-16T09:35:12Z"
      }
    }
  }
}
```

**Use cases:**
- Historical analysis
- Trend charting
- Automated reporting
- API integration

---

## 🛠️ Troubleshooting with Monitor

### Problem: Low success rate after deployment

```bash
# Check today vs yesterday
python3 monitor_data_freshness.py --days 2

# Example output:
# ❌ 2026-01-16: 35/92 apps (38.0%)  ← After deployment
# ✅ 2026-01-15: 74/92 apps (80.4%)  ← Before deployment
```

**Action:** Rollback deployment, check logs

---

### Problem: Specific data type always missing

```bash
# Check recent trend
python3 monitor_data_freshness.py --days 7

# If "purchases" consistently low:
# ⚠️  purchases: 72/92 apps (78.3%)
```

**Action:** 
- Normal if apps don't have purchases
- Check if specific apps always missing
- Review Apple API response for that data type

---

### Problem: Registry ages increasing

```bash
# Check registry distribution
python3 monitor_data_freshness.py

# Age distribution:
#   0-30d: 0 apps   ← Should have some here
#   31-60d: 92 apps ← All registries aging
#   61-90d: 0 apps
#   90+d: 0 apps
```

**Action:**
- Registries should refresh occasionally
- Check if new requests are being created
- Review registry trust period (currently 180 days)

---

## 📅 Recommended Monitoring Schedule

### Daily (After Cron Run)
```bash
# 10:00 UTC - Check yesterday's data
ssh ec2-user@<PRODUCTION_IP>
cd /data/apple-analytics
python3 monitor_data_freshness.py

# Look for:
# - Success rate ≥ 80%
# - No new failed apps
# - Registry ages stable
```

### Weekly (Monday Morning)
```bash
# Check 7-day trend
python3 monitor_data_freshness.py --days 7

# Look for:
# - Consistent 80%+ success
# - No degradation pattern
# - Registry distribution healthy
```

### Monthly (First of Month)
```bash
# Check 30-day trend
python3 monitor_data_freshness.py --days 30

# Generate report:
# - Average success rate
# - Data type coverage
# - Registry aging analysis
# - Apps that consistently fail
```

---

## 🎯 Key Metrics to Track

### 1. Success Rate (Primary KPI)
- **Target:** ≥ 80% (75/92 apps)
- **Calculation:** `apps_with_fresh_curated_data / total_apps`
- **Alert threshold:** < 70%

### 2. Data Type Coverage
- **Target:** 
  - downloads, engagement, sessions: ≥ 90%
  - installs: ≥ 85%
  - purchases: ≥ 70%
- **Alert threshold:** Any type < 60%

### 3. Registry Health
- **Target:** 
  - Average age: 30-90 days
  - No registries > 180 days
- **Alert threshold:** Average > 120 days

### 4. Failed Apps Consistency
- **Target:** < 20 apps consistently failing
- **Alert threshold:** Same 30+ apps failing for 3+ days

---

## 📚 Related Documentation

- `DEPLOYMENT_PLAN_JAN16.md` - Current deployment plan
- `SLACK_MESSAGE_PREVIEW.md` - Slack logging format
- `unified_etl.py` - Main ETL pipeline
- `apple_analytics_client.py` - Client with registry logic

---

## 🔧 Advanced Usage

### Custom App List
```bash
# Check specific apps only
export APP_IDS="1506886061,6443744460,1446546237"
python3 monitor_data_freshness.py
```

### Programmatic Access
```python
from monitor_data_freshness import DataFreshnessMonitor

monitor = DataFreshnessMonitor()
report = monitor.generate_daily_report('2026-01-16', '2026-01-15')

# Access results
success_rate = report['apps_with_fresh_curated_data'] / report['total_apps']
failed_apps = report['apps_missing_data']
```

---

**Created:** January 16, 2026  
**Last Updated:** January 16, 2026  
**Status:** Ready for production use ✅
