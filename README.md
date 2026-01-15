# 🍎 Apple Analytics ETL Pipeline

> **Status**: ✅ Production Ready  
> **Last Updated**: January 16, 2026  
> **Apps**: 92 configured | 75 with active data  
> **Monitoring**: ✅ Automated Slack reports enabled

Automated ETL pipeline for extracting Apple App Store Connect Analytics data and loading it into AWS Athena with comprehensive monitoring and alerting.

---

## 🚀 Quick Start

```bash
# 1. Navigate to directory
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# 2. Activate virtual environment
source ../.venv/bin/activate

# 3. Run ETL (extracts yesterday's data for all 92 apps)
python3 unified_etl.py
```

---

## 📊 Current Status (January 16, 2026)

| Metric | Value | Status |
|--------|-------|--------|
| **Success Rate** | 81.5% (75/92 apps) | ✅ Healthy |
| **Registry Trust Period** | 180 days (6 months) | ✅ Extended |
| **Monitoring** | Automated Slack reports | ✅ Active |
| **Cron Schedule** | Daily at 09:30 UTC | ✅ Running |

### Athena Tables

| Athena Table | Rows | Apps | Status |
|--------------|------|------|--------|
| `curated_downloads` | 7,296,780 | 74 | ✅ 0 duplicates |
| `curated_engagement` | 4,948,856 | 57 | ✅ 0 duplicates |
| `curated_sessions` | 503,458 | 63 | ✅ 0 duplicates |
| `curated_installs` | 450,691 | 57 | ✅ 0 duplicates |
| `curated_purchases` | 437,988 | 65 | ✅ 0 duplicates |

### Recent Improvements (Jan 16, 2026)
- ✅ **Extended registry trust period** from 60 to 180 days
- ✅ **Automated Slack monitoring** with daily reports
- ✅ **Enhanced logging** with pipe-delimited structured format
- ✅ **Fixed 403 errors** by avoiding unnecessary API verification

---

## 📁 Project Structure

```
Apple-Analytics/
├── unified_etl.py               # 🔑 Daily ETL (ONGOING requests, daily data)
├── unified_onetime_etl.py       # 📅 Backfill ETL (ONE_TIME_SNAPSHOT requests)
├── monitor_data_freshness.py   # 📊 Data freshness monitoring & Slack alerts
├── daily_cron.sh                # Cron wrapper for daily automation (09:30 UTC)
├── startup_verification.sh      # Health check script
├── .env                         # Configuration (not in git)
├── .env.template                # Config template
│
├── src/extract/
│   ├── apple_analytics_client.py       # Apple API client with JWT auth
│   ├── focused_data_extractor.py       # Data extraction logic
│   └── apple_request_status_checker.py # Request status monitoring
│
├── src/transform/
│   └── apple_analytics_data_curator_production.py  # CSV → Parquet conversion
│
├── src/load/
│   └── athena_table_manager_production.py  # Athena partition management
│
├── src/orchestration/
│   └── unified_production_etl.py        # Orchestration logic
│
├── sql/
│   └── setup_curated_tables.sql   # Athena table definitions (6 data types)
│
├── config/
│   └── etl_config.json            # Configuration
│
├── docs/                          # 📚 Comprehensive documentation
│   ├── INDEX.md                   # Navigation guide
│   ├── DEPLOYMENT_COMPLETE_JAN16.md           # Deployment summary
│   ├── DATA_FRESHNESS_MONITORING_GUIDE.md     # Monitoring guide
│   ├── SLACK_INTEGRATION_COMPLETE.md          # Slack setup
│   ├── MONITORING_COMPLETE_SUMMARY.md         # Monitoring overview
│   └── ... (11 total documentation files)
│
├── logs/                          # ETL execution logs
└── archive/                       # Legacy/archived files
```

---

## 🔧 Usage

### 📊 Daily Monitoring (Automated)
Pipeline automatically sends **Slack reports** at **09:35 UTC daily** after ETL completes.

```bash
# Manual data freshness check
python3 monitor_data_freshness.py

# Send Slack report manually
python3 monitor_data_freshness.py --slack

# Check specific date
python3 monitor_data_freshness.py --date 2026-01-15

# 7-day trend analysis
python3 monitor_data_freshness.py --days 7
```

See [DATA_FRESHNESS_MONITORING_GUIDE.md](DATA_FRESHNESS_MONITORING_GUIDE.md) for complete monitoring documentation.

### Daily Automated Run (ONGOING Requests)
Pipeline runs automatically at **09:30 UTC daily** via cron using `unified_etl.py`.
- Creates one ONGOING request per app (reused forever)
- Extracts yesterday's data only
- No 409 conflicts via S3 registry
- **180-day registry trust period** (extended from 60 days)

### Manual Commands - Daily ETL (ONGOING)

```bash
# Full ETL for yesterday (all 92 apps)
python3 unified_etl.py

# Specific date
python3 unified_etl.py --date 2025-12-20

# Specific app
python3 unified_etl.py --app-id 1506886061

# Transform only (re-curate existing raw data)
python3 unified_etl.py --transform-only --date 2025-12-20

# Load only (refresh Athena partitions)
python3 unified_etl.py --load-only

# Backfill last N days
python3 unified_etl.py --backfill --days 30
```

### Manual Commands - ONE_TIME_SNAPSHOT Backfill

```bash
# One-time snapshot for specific date range
python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-30

# One-time snapshot for specific app
python3 unified_onetime_etl.py --app-id 1506886061 --start-date 2025-11-01 --end-date 2025-11-05

# One-time snapshot for all apps, specific range
python3 unified_onetime_etl.py --start-date 2025-11-15 --end-date 2025-11-20

# Dry run (check what would be processed)
python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-05 --dry-run
```

### Verification

```bash
# Run health check
./startup_verification.sh

# Check daily ETL logs
tail -f logs/unified_etl_$(date +%Y%m%d).log

# Check monitoring logs
tail -f logs/monitor_$(date +%Y%m%d).log

# Check backfill ETL logs
tail -f logs/unified_onetime_etl_$(date +%Y%m%d).log

# Verify cron is set up
crontab -l | grep "unified_etl\|monitor_data_freshness"

# View latest data freshness report
cat logs/data_freshness_$(date -v-1d +%Y-%m-%d).json | jq
```

---

## 🏗️ Architecture

```
┌────────────────────┐
│   Apple API        │
│  (App Analytics)   │
└─────────┬──────────┘
          │
    ┌─────▼──────────────────────────────────────────┐
    │         Request Type Selection                 │
    └─────┬────────────────────┬────────────────────┘
          │                    │
    ┌─────▼──────┐       ┌─────▼──────┐
    │  ONGOING   │       │ ONE_TIME   │
    │ (Daily)    │       │ (Backfill) │
    └─────┬──────┘       └─────┬──────┘
          │                    │
    ┌─────▼────────────────────▼─────────────┐
    │   Extract to S3 Raw (CSV)              │
    │   s3://skidos-apptrack/appstore/raw/   │
    └─────┬────────────────────────────────┘
          │
    ┌─────▼────────────────────────────────────┐
    │  Transform to S3 Curated (Parquet)       │
    │  s3://skidos-apptrack/appstore/curated/  │
    └─────┬────────────────────────────────────┘
          │
    ┌─────▼─────────────────────────────────┐
    │  Load to Athena Tables (6 types)      │
    │  - curated_downloads                  │
    │  - curated_engagement                 │
    │  - curated_sessions                   │
    │  - curated_installs                   │
    │  - curated_purchases                  │
    │  - curated_reviews                    │
    └───────────────────────────────────────┘
```

---

## 💾 S3 Storage Locations

### Raw Data (CSV Files)
Unprocessed data directly from Apple API, organized by data type and partition.

```
s3://skidos-apptrack/appstore/raw/
├── downloads/
│   └── dt=2025-12-20/
│       ├── app_id=1506886061/
│       │   ├── downloads_segment_xyz.csv
│       │   └── downloads_segment_abc.csv
│       └── app_id=1159612010/
│           └── downloads_segment_def.csv
│
├── engagement/
│   └── dt=2025-12-20/
│       ├── app_id=1506886061/
│       │   └── engagement_discovery_segment.csv
│       └── app_id=1159612010/
│           └── engagement_impression_segment.csv
│
├── sessions/
│   └── dt=2025-12-20/app_id=XXXX/*.csv
│
├── installs/
│   └── dt=2025-12-20/app_id=XXXX/*.csv
│
├── purchases/
│   └── dt=2025-12-20/app_id=XXXX/*.csv
│
└── reviews/
    └── dt=2025-12-20/app_id=XXXX/*.csv
```

**File Naming**: `{report_type}_{segment_id}.csv`  
**Format**: Tab-separated values (TSV)  
**Compression**: None (stored as plain text)

### Curated Data (Parquet Files)
Processed, deduplicated, and optimized data for analytics.

```
s3://skidos-apptrack/appstore/curated/
├── downloads/
│   └── dt=2025-12-20/
│       ├── app_id=1506886061/
│       │   └── data.parquet
│       └── app_id=1159612010/
│           └── data.parquet
│
├── engagement/
│   └── dt=2025-12-20/app_id=XXXX/data.parquet
│
├── sessions/
│   └── dt=2025-12-20/app_id=XXXX/data.parquet
│
├── installs/
│   └── dt=2025-12-20/app_id=XXXX/data.parquet
│
├── purchases/
│   └── dt=2025-12-20/app_id=XXXX/data.parquet
│
└── reviews/
    └── dt=2025-12-20/app_id=XXXX/data.parquet
```

**File Format**: Apache Parquet (columnar, compressed)  
**Deduplication**: Applied during transformation  
**Partitioning**: By date (dt) and app_id for optimal query performance

### Request Registry (S3)
Tracks created ONGOING and ONE_TIME_SNAPSHOT requests to avoid 409 conflicts.

```
s3://skidos-apptrack/analytics_requests/registry/
├── app_id=1506886061/
│   ├── ongoing.json          # ONGOING request ID (reused daily)
│   └── one_time_snapshot.json # Latest ONE_TIME_SNAPSHOT ID
│
└── app_id=1159612010/
    ├── ongoing.json
    └── one_time_snapshot.json
```

**Content Example**:
```json
{
  "app_id": "1506886061",
  "access_type": "ONGOING",
  "request_id": "abc123-def456-ghi789",
  "created_at": "2025-12-01T10:30:00+00:00"
}
```

---

## 📊 Athena Tables & Storage

### Database: `appstore`

All 6 data types stored in Athena for SQL querying with partition pruning.

#### Table: `curated_downloads`
Downloads analytics by territory, device, platform version, etc.
```
Location: s3://skidos-apptrack/appstore/curated/downloads/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Columns: metric_date, app_id, app_name, territory, download_type, 
         source_type, device, platform_version, total_downloads
Row Count: ~7.3M | Status: ✅ 0 duplicates
```

#### Table: `curated_engagement`
Engagement metrics (impressions, sessions, etc.)
```
Location: s3://skidos-apptrack/appstore/curated/engagement/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Columns: metric_date, app_id, app_name, territory, device, 
         impression_count, feature_engagement
Row Count: ~4.9M | Status: ✅ 0 duplicates
```

#### Table: `curated_sessions`
App session data
```
Location: s3://skidos-apptrack/appstore/curated/sessions/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Row Count: ~503K | Status: ✅ 0 duplicates
```

#### Table: `curated_installs`
Installation metrics
```
Location: s3://skidos-apptrack/appstore/curated/installs/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Row Count: ~450K | Status: ✅ 0 duplicates
```

#### Table: `curated_purchases`
In-app purchase analytics
```
Location: s3://skidos-apptrack/appstore/curated/purchases/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Row Count: ~437K | Status: ✅ 0 duplicates
```

#### Table: `curated_reviews`
App store reviews and ratings
```
Location: s3://skidos-apptrack/appstore/curated/reviews/
Partitions: dt (YYYY-MM-DD), app_id (BIGINT)
Row Count: ~12K | Status: ✅ 0 duplicates
```

---

## 📊 Monitoring & Alerting

### Automated Slack Reports

Daily reports are automatically sent to Slack at **09:35 UTC** (5 minutes after ETL completes).

**Report includes:**
- ✅ Overall success rate and status (HEALTHY/DEGRADED/CRITICAL)
- 📊 Data type coverage breakdown (downloads, engagement, sessions, etc.)
- 📖 Registry health metrics (average age, oldest registry)
- ⚠️ List of apps with missing data
- 📈 Trends and anomalies

**Example Slack Message:**
```
✅ Apple Analytics ETL Report - 2026-01-16

Status: HEALTHY
Success Rate: 75/92 apps (81.5%)

📊 Data Type Coverage:
✅ downloads      75/92 (81.5%)
✅ engagement     80/92 (87.0%)
✅ sessions       78/92 (84.8%)
✅ installs       82/92 (89.1%)
⚠️  purchases     65/92 (70.7%)

✅ Registry Health:
• Average age: 48.3 days
• Oldest: 50 days
• Registries: 75/92
```

### Data Freshness Validation

The monitoring system validates:
1. **RAW data files** - CSV files in S3 by processing_date
2. **CURATED data files** - Parquet files in S3 by metric_date  
3. **Registry health** - Age and last_verified timestamps
4. **Historical trends** - Success rate over time

See [DATA_FRESHNESS_MONITORING_GUIDE.md](DATA_FRESHNESS_MONITORING_GUIDE.md) for complete details.

### Key Performance Indicators

| KPI | Target | Alert Threshold |
|-----|--------|----------------|
| Success Rate | ≥ 80% | < 70% |
| downloads/engagement/sessions | ≥ 90% | < 60% |
| installs | ≥ 85% | < 60% |
| purchases | ≥ 70% | < 50% |
| Average Registry Age | 30-90 days | > 120 days |

---

## 📖 Full Documentation

### Quick Start Guides
- **[INDEX.md](INDEX.md)** - Complete navigation guide
- **[MONITORING_COMPLETE_SUMMARY.md](MONITORING_COMPLETE_SUMMARY.md)** - Monitoring overview
- **[DATA_FRESHNESS_MONITORING_GUIDE.md](DATA_FRESHNESS_MONITORING_GUIDE.md)** - Complete monitoring usage

### Deployment & Analysis
- **[DEPLOYMENT_COMPLETE_JAN16.md](DEPLOYMENT_COMPLETE_JAN16.md)** - Latest deployment summary
- **[FINAL_ROOT_CAUSE_CONFIRMED.md](FINAL_ROOT_CAUSE_CONFIRMED.md)** - Root cause analysis
- **[PRODUCTION_ANALYSIS_JAN15.md](PRODUCTION_ANALYSIS_JAN15.md)** - Performance analysis

### Slack Integration
- **[SLACK_INTEGRATION_COMPLETE.md](SLACK_INTEGRATION_COMPLETE.md)** - Slack setup guide
- **[SLACK_MESSAGE_PREVIEW.md](SLACK_MESSAGE_PREVIEW.md)** - Example messages
- **[SLACK_LOGGING_QUICK_REFERENCE.md](SLACK_LOGGING_QUICK_REFERENCE.md)** - Quick reference

### Legacy Documentation
For comprehensive pipeline documentation including:
- Complete pipeline flow diagrams
- Deduplication logic details  
- Troubleshooting guide
- FAQ

See: **[COMPLETE_PIPELINE_DOCUMENTATION.md](./COMPLETE_PIPELINE_DOCUMENTATION.md)**

---

## ⚙️ Configuration

Required environment variables in `.env`:

```bash
# Apple API
ASC_ISSUER_ID=xxx
ASC_KEY_ID=xxx
ASC_P8_PATH=/path/to/AuthKey.p8

# AWS
AWS_REGION=us-east-1
S3_BUCKET=skidos-apptrack

# Apps (92 app IDs)
APP_IDS=1506886061,1159612010,...
```

---

## 🔍 Sample Athena Queries

### Downloads Analytics

```sql
-- Daily downloads by app (last 30 days)
SELECT 
    app_name,
    metric_date,
    territory,
    SUM(total_downloads) as daily_downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-11-20'
  AND metric_date >= '2025-11-20'
  AND app_id = 1506886061
GROUP BY app_name, metric_date, territory
ORDER BY metric_date DESC, daily_downloads DESC;

-- Top countries by downloads
SELECT 
    territory,
    SUM(total_downloads) as total_downloads,
    COUNT(DISTINCT metric_date) as days
FROM appstore.curated_downloads
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY territory
ORDER BY total_downloads DESC
LIMIT 20;

-- Download type breakdown
SELECT 
    metric_date,
    download_type,
    SUM(total_downloads) as count
FROM appstore.curated_downloads
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date, download_type
ORDER BY metric_date DESC;

-- Device/Platform analysis
SELECT 
    metric_date,
    device,
    platform_version,
    SUM(total_downloads) as downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date, device, platform_version
ORDER BY downloads DESC;
```

### Engagement Analytics

```sql
-- Daily impressions and engagement
SELECT 
    app_name,
    metric_date,
    SUM(impression_count) as impressions,
    SUM(feature_engagement) as engagement
FROM appstore.curated_engagement
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY app_name, metric_date
ORDER BY metric_date DESC;

-- Engagement by territory
SELECT 
    territory,
    SUM(impression_count) as total_impressions,
    SUM(feature_engagement) as total_engagement,
    ROUND(100.0 * SUM(feature_engagement) / SUM(impression_count), 2) as engagement_rate
FROM appstore.curated_engagement
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY territory
ORDER BY total_impressions DESC;
```

### Installs & Purchases

```sql
-- Daily installs
SELECT 
    metric_date,
    SUM(total_installs) as installs
FROM appstore.curated_installs
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date
ORDER BY metric_date DESC;

-- Purchase revenue analysis
SELECT 
    metric_date,
    SUM(proceeds) as revenue,
    COUNT(DISTINCT territory) as countries
FROM appstore.curated_purchases
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date
ORDER BY revenue DESC;
```

### Multi-App Comparison

```sql
-- Compare top 5 apps by downloads
SELECT 
    app_name,
    SUM(total_downloads) as total_downloads,
    COUNT(DISTINCT metric_date) as days,
    ROUND(AVG(total_downloads), 2) as avg_daily_downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-12-01'
GROUP BY app_name
ORDER BY total_downloads DESC
LIMIT 5;

-- Cross-app performance matrix
SELECT 
    app_id,
    app_name,
    COUNT(DISTINCT metric_date) as data_days,
    COUNT(DISTINCT territory) as countries,
    SUM(total_downloads) as total_downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-12-01'
GROUP BY app_id, app_name
HAVING COUNT(DISTINCT metric_date) > 0
ORDER BY total_downloads DESC;
```

### Data Quality Checks

```sql
-- Check for duplicates in downloads (should return 0)
SELECT 
    COUNT(*) - COUNT(DISTINCT CONCAT(
        CAST(metric_date AS VARCHAR), 
        CAST(app_id AS VARCHAR), 
        territory, 
        download_type, 
        source_type, 
        device, 
        platform_version
    )) as duplicate_count
FROM appstore.curated_downloads
WHERE dt = '2025-12-20';

-- Verify all partitions are loaded
SELECT 
    dt,
    COUNT(DISTINCT app_id) as apps_with_data,
    SUM(total_downloads) as total_downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-12-15'
GROUP BY dt
ORDER BY dt DESC;

-- Check data freshness
SELECT 
    MAX(metric_date) as latest_data_date,
    MIN(metric_date) as oldest_data_date,
    COUNT(DISTINCT metric_date) as days_available
FROM appstore.curated_downloads
WHERE dt >= '2025-11-20';
```

### Sessions & Reviews

```sql
-- Session trends
SELECT 
    metric_date,
    SUM(session_count) as sessions,
    SUM(crash_count) as crashes
FROM appstore.curated_sessions
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date
ORDER BY metric_date DESC;

-- App reviews sentiment
SELECT 
    metric_date,
    ROUND(AVG(rating), 2) as avg_rating,
    COUNT(review_id) as total_reviews,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) as positive_reviews
FROM appstore.curated_reviews
WHERE dt >= '2025-12-01'
  AND app_id = 1506886061
GROUP BY metric_date
ORDER BY metric_date DESC;
```

---

## 📝 Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2025-12-20 | v3.0 | Added `unified_onetime_etl.py` for ONE_TIME_SNAPSHOT backfills, comprehensive S3/Athena documentation, expanded query examples |
| 2025-12-01 | v2.0 | Fixed deduplication, consolidated docs, repository cleanup, 6 data types |
| 2025-11-28 | v1.5 | Unified ETL script, cron automation at 6 AM |
| 2025-11-27 | v1.0 | Initial production deployment |

---

## 🔗 Related Documentation

- **[COMPLETE_PIPELINE_DOCUMENTATION.md](./COMPLETE_PIPELINE_DOCUMENTATION.md)** - Full architecture, deduplication logic, troubleshooting
- **[unified_onetime_etl.py](./unified_onetime_etl.py)** - ONE_TIME_SNAPSHOT ETL script for backfills
- **[unified_etl.py](./unified_etl.py)** - Daily ETL script using ONGOING requests

---

## 📞 Support & Troubleshooting

### Common Issues

**409 Conflict Error**
- Cause: Trying to create duplicate request for same app
- Solution: Script automatically checks S3 registry and reuses existing request
- Location: `s3://skidos-apptrack/analytics_requests/registry/`

**Partition Pruning Errors**
- Cause: Missing `dt` or `app_id` in WHERE clause for raw tables
- Solution: Always include: `WHERE dt >= 'YYYY-MM-DD' AND app_id = NNNN`

**Data Not Appearing in Athena**
- Check: Raw CSV files in S3 `appstore/raw/{type}/dt=YYYY-MM-DD/`
- Check: Parquet files in S3 `appstore/curated/{type}/dt=YYYY-MM-DD/`
- Check: Run `aws s3 ls s3://skidos-apptrack/appstore/curated/ --recursive | head -20`

**Slow Athena Queries**
- Add partition constraints: `WHERE dt >= 'YYYY-MM-DD'`
- Add app_id filter: `AND app_id = 1506886061`
- Use smaller date ranges for large apps

### Logs & Monitoring

```bash
# Daily ETL logs
tail -f logs/unified_etl_$(date +%Y%m%d).log

# Backfill ETL logs
tail -f logs/unified_onetime_etl_$(date +%Y%m%d).log

# Recent errors
grep -i "error\|failed\|409" logs/unified_etl_*.log | tail -20

# Check S3 registry
aws s3 ls s3://skidos-apptrack/analytics_requests/registry/
```

---

**License**: Internal use only - SKIDOS
