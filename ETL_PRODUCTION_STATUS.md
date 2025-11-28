# Apple Analytics ETL Pipeline - Production Status

**Last Updated**: 2025-11-28  
**Status**: ✅ Production Ready

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
         │                 │  Batch Curator   │
         │                 │  (CSV→Parquet)   │
         │                 └──────────────────┘
         │                          │
         │                          ▼
         │                 ┌──────────────────┐
         └────────────────▶│ Curated Parquet  │
                           │   (Optimized)    │
                           └──────────────────┘
```

## ✅ What's Implemented

### 1. Daily ETL Pipeline (`daily_etl.py`)
- **ONGOING requests** (not ONE_TIME_SNAPSHOT) - avoids 409 conflicts
- Automatic extraction of yesterday's data
- Registry-based request management (reuses existing requests)
- All 92 apps configured
- Data types: downloads, engagement, sessions, installs, purchases, reviews

### 2. Batch Curator (`batch_curate.py`)
- Transforms raw CSV to optimized Parquet format
- Processes all 6 data types:
  - ✅ Downloads
  - ✅ Engagement
  - ✅ Sessions
  - ✅ Installs
  - ✅ Purchases
  - ✅ Reviews (JSON parsing)

### 3. Historical Backfill (`historical_backfill.py`)
- Supports date range extraction
- Command-line arguments for flexibility
- Progress tracking

### 4. Cron Automation
- `run_daily_pipeline.sh` - Complete daily automation script
- `cron_setup.sh` - Interactive cron job setup
- Automatic log rotation (30 days)

### 5. S3 Structure
```
s3://skidos-apptrack/appstore/
├── raw/
│   ├── downloads/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── engagement/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── sessions/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── installs/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   ├── purchases/dt=YYYY-MM-DD/app_id=NNNN/*.csv
│   └── reviews/dt=YYYY-MM-DD/app_id=NNNN/*.json
└── curated/
    ├── downloads/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── engagement/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── sessions/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── installs/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    ├── purchases/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
    └── reviews/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
```

### 6. Athena Tables (Curated)
| Table | Status | Rows |
|-------|--------|------|
| curated_downloads | ✅ Working | 6.3M+ |
| curated_engagement | ✅ Working | 10.3M+ |
| curated_sessions | ✅ Schema Ready | Processing |
| curated_installs | ✅ Schema Ready | Processing |
| curated_purchases | ✅ Working | 324K+ |
| curated_reviews | ✅ Working | 4.2K+ |

## 🔧 Production Usage

### Daily Automated Run (Cron)
```bash
# Install cron job (runs at 6 AM daily)
./cron_setup.sh

# Manual check
crontab -l
```

### Manual Execution
```bash
# Extract yesterday's data
cd Apple-Analytics
python3 daily_etl.py

# Curate to Parquet
cd ..
python3 batch_curate.py

# Historical backfill (last 30 days)
python3 historical_backfill.py --days 30
```

### Monitor Pipeline
```bash
# Check logs
tail -f logs/daily_pipeline_$(date +%Y-%m-%d).log

# Query Athena
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) FROM appstore.curated_downloads" \
    --result-configuration OutputLocation=s3://skidos-apptrack/Athena-Output/
```

## 📁 Key Files

| File | Purpose |
|------|---------|
| `daily_etl.py` | Extract from Apple API (ONGOING requests) |
| `batch_curate.py` | Transform CSV→Parquet |
| `historical_backfill.py` | Backfill historical data |
| `run_daily_pipeline.sh` | Complete daily automation |
| `cron_setup.sh` | Cron job installer |
| `src/extract/apple_analytics_client.py` | Apple API client |

## ⚠️ Known Limitations

1. **Raw Tables**: Raw Athena tables have partition projection issues (curated tables work fine)
2. **Historical Data**: Apple API provides up to 365 days of historical data
3. **Rate Limits**: Apple API has rate limits; large backfills may take time

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
```

## ✅ Production Checklist

- [x] ONGOING request implementation (no 409 conflicts)
- [x] Daily extraction automation
- [x] S3 partitioned storage
- [x] CSV→Parquet curation
- [x] All 6 data types supported
- [x] Reviews JSON parsing
- [x] Athena table schemas
- [x] Cron job setup script
- [x] Log rotation
- [x] Historical backfill support
- [ ] Alerting/monitoring (optional)
- [ ] Dashboard integration (optional)

---

**Ready for Production Deployment** ✅
