# Apple Analytics ETL Pipeline - Production Status

**Last Updated**: 2025-12-01  
**Status**: ✅ Production Ready - All Data Types Working

## Overview

This ETL pipeline extracts data from Apple App Store Connect Analytics API and loads it into AWS S3/Athena for analysis.

## Architecture

```
┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Apple Analytics    │───▶│    S3 Bucket     │───▶│    Athena       │
│       API           │    │  (Raw CSV/JSON)  │    │  (SQL Queries)  │
└─────────────────────┘    └──────────────────┘    └─────────────────┘
         │                          │
         │                          ▼
         │                 ┌──────────────────┐
         │                 │ Unified ETL      │
         │                 │ (Extract→       │
         │                 │  Transform→Load) │
         │                 └──────────────────┘
         │                          │
         │                          ▼
         │                 ┌──────────────────┐
         └────────────────▶│ Curated Parquet  │
                           │   (Deduplicated) │
                           └──────────────────┘
```

## ✅ What's Implemented

### 1. Unified ETL Pipeline (`unified_etl.py`)
- **Single script** for complete ETL (Extract → Transform → Load)
- **ONGOING requests** (not ONE_TIME_SNAPSHOT) - avoids 409 conflicts
- **Deduplication** at raw level before transformation
- **Aggregation** by all dimension columns with metric summing
- Automatic extraction of yesterday's data
- Registry-based request management (reuses existing requests)
- All 92 apps configured
- Data types: downloads, engagement, sessions, installs, purchases

### 2. Deduplication Logic
Apple's API sometimes provides overlapping data segments. The pipeline handles this:
```python
def _transform_dataframe(self, data_type, df, app_id, target_date):
    # First, deduplicate raw data (Apple's overlapping segments)
    df = df.drop_duplicates()
    
    # Transform to curated schema
    curated = pd.DataFrame({...})
    
    # Aggregate by all dimensions, sum metrics
    return curated.groupby(group_cols, as_index=False).agg({'total_downloads': 'sum'})
```

### 3. S3 Structure
```
s3://skidos-apptrack/appstore/
├── raw/
│   ├── downloads/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── engagement/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── sessions/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── installs/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   └── purchases/dt=YYYY-MM-DD/app_id=NNNN/*.csv
└── curated/
    ├── downloads/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── engagement/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── sessions/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── installs/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    └── purchases/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
```

### 4. Athena Tables (Curated) - As of 2025-12-01
| Table | Status | Rows | Apps | Partitions |
|-------|--------|------|------|------------|
| curated_downloads | ✅ Working | 7,296,780 | 74 | 7 |
| curated_engagement | ✅ Working | 4,948,856 | 57 | 3 |
| curated_sessions | ✅ Working | 503,458 | 63 | 2 |
| curated_installs | ✅ Working | 450,691 | 57 | 2 |
| curated_purchases | ✅ Working | 437,988 | 65 | 7 |

### 5. Cron Automation
```bash
# Installed cron job (runs at 6 AM daily)
0 6 * * * /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics/daily_cron.sh >> logs/cron.log 2>&1
```

## 🔧 Production Usage

### Daily Automated Run (Cron)
The pipeline runs automatically at 6 AM daily via cron.

### Manual Execution
```bash
cd Apple-Analytics

# Full ETL (extract + transform + load)
python3 unified_etl.py

# Specific date
python3 unified_etl.py --date 2025-11-28

# Transform-only (re-curate existing raw data)
python3 unified_etl.py --transform-only --date 2025-11-28

# Load-only (refresh Athena partitions)
python3 unified_etl.py --load-only

# Backfill last 30 days
python3 unified_etl.py --backfill --days 30
```

### Monitor Pipeline
```bash
# Check logs
tail -f logs/unified_etl_$(date +%Y%m%d).log

# Query Athena
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) FROM appstore.curated_downloads" \
    --result-configuration OutputLocation=s3://skidos-apptrack/Athena-Output/
```

## 📁 Key Files

| File | Purpose |
|------|---------|
| `unified_etl.py` | Complete ETL (Extract→Transform→Load) |
| `daily_cron.sh` | Cron wrapper script |
| `src/extract/apple_analytics_client.py` | Apple API client |

## 🔐 Required Environment Variables

```bash
# Apple API
ASC_ISSUER_ID=xxx
ASC_KEY_ID=xxx
ASC_P8_PATH=/path/to/AuthKey.p8

# AWS
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
S3_BUCKET=skidos-apptrack

# Apps
APP_IDS=1506886061,1159612010,...
```

## 📊 Sample Queries

```sql
-- Daily downloads by app
SELECT app_name, metric_date, SUM(total_downloads) as downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-11-01'
GROUP BY app_name, metric_date
ORDER BY downloads DESC;

-- Revenue by territory
SELECT territory, SUM(proceeds_usd) as revenue
FROM appstore.curated_purchases
GROUP BY territory
ORDER BY revenue DESC;

-- App engagement
SELECT app_name, SUM(impressions) as impressions
FROM appstore.curated_engagement
GROUP BY app_name
ORDER BY impressions DESC;

-- Check for duplicates (should return 0)
SELECT COUNT(*) - COUNT(DISTINCT CONCAT(
    CAST(metric_date AS VARCHAR), app_name, CAST(app_id AS VARCHAR), 
    territory, download_type, source_type, device, platform_version
)) as duplicates
FROM appstore.curated_downloads
WHERE dt = '2025-11-28';
```

## ✅ Production Checklist

- [x] ONGOING request implementation (no 409 conflicts)
- [x] Daily extraction automation
- [x] S3 partitioned storage
- [x] CSV→Parquet curation
- [x] Deduplication before transformation
- [x] Aggregation by dimensions
- [x] All 5 data types supported
- [x] Athena table schemas
- [x] Cron job installed
- [x] Unified ETL script
- [x] Transform-only mode
- [x] Load-only mode
- [x] Backfill support
- [ ] Alerting/monitoring (optional)
- [ ] Dashboard integration (optional)

---

**Ready for Production Deployment** ✅
