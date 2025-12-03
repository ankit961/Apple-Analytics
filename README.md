# 🍎 Apple Analytics ETL Pipeline

> **Status**: ✅ Production Ready  
> **Last Updated**: December 3, 2025  
> **Apps**: 92 configured | 74 with active data

Automated ETL pipeline for extracting Apple App Store Connect Analytics data and loading it into AWS Athena.

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

## 📊 Current Status (December 3, 2025)

| Athena Table | Rows | Apps | Status |
|--------------|------|------|--------|
| `curated_downloads` | 8,423,289 | 74 | ✅ 0 duplicates |
| `curated_engagement` | 5,287,303 | 57 | ✅ 0 duplicates |
| `curated_sessions` | 554,199 | 63 | ✅ 0 duplicates |
| `curated_installs` | 509,292 | 57 | ✅ 0 duplicates |
| `curated_purchases` | 455,693 | 65 | ✅ 0 duplicates |
| `curated_reviews` | 4,250 | - | ✅ 0 duplicates |

---

## 📁 Project Structure

```
Apple-Analytics/
├── unified_etl.py           # 🔑 Main ETL script (Extract → Transform → Load)
├── daily_cron.sh            # Cron wrapper for automation
├── startup_verification.sh  # Health check script
├── .env                     # Configuration (not in git)
├── .env.template            # Config template
│
├── src/extract/
│   └── apple_analytics_client.py  # Apple API client with JWT auth
│
├── sql/
│   └── setup_curated_tables.sql   # Athena table definitions
│
├── logs/                    # ETL execution logs
└── config/                  # Additional configuration
```

---

## 🔧 Usage

### Daily Automated Run
Pipeline runs automatically at **6 AM daily** via cron.

### Manual Commands

```bash
# Full ETL for yesterday
python3 unified_etl.py

# Specific date
python3 unified_etl.py --date 2025-11-28

# Specific app
python3 unified_etl.py --app-id 1506886061

# Transform only (re-curate existing raw data)
python3 unified_etl.py --transform-only --date 2025-11-28

# Load only (refresh Athena partitions)
python3 unified_etl.py --load-only

# Backfill last N days
python3 unified_etl.py --backfill --days 30
```

### Verification

```bash
# Run health check
./startup_verification.sh

# Check logs
tail -f logs/unified_etl_$(date +%Y%m%d).log

# Verify cron
crontab -l
```

---

## 🏗️ Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌───────────┐
│ Apple API    │───▶│ S3 Raw       │───▶│ S3 Curated   │───▶│ Athena    │
│ (Analytics)  │    │ (CSV)        │    │ (Parquet)    │    │ (Tables)  │
└──────────────┘    └──────────────┘    └──────────────┘    └───────────┘
```

**S3 Structure:**
```
s3://skidos-apptrack/appstore/
├── raw/{type}/dt=YYYY-MM-DD/app_id=NNNN/*.csv
└── curated/{type}/dt=YYYY-MM-DD/app_id=NNNN/data.parquet
```

---

## 📖 Full Documentation

For comprehensive documentation including:
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

```sql
-- Daily downloads
SELECT app_name, metric_date, SUM(total_downloads) as downloads
FROM appstore.curated_downloads
WHERE dt >= '2025-11-01'
GROUP BY app_name, metric_date
ORDER BY downloads DESC;

-- Check for duplicates (should return 0)
SELECT COUNT(*) - COUNT(DISTINCT CONCAT(
    CAST(metric_date AS VARCHAR), app_name, CAST(app_id AS VARCHAR), 
    territory, download_type, source_type, device, platform_version
)) as duplicates
FROM appstore.curated_downloads
WHERE dt = '2025-11-28';
```

---

## 📝 Changelog

| Date | Changes |
|------|---------|
| 2025-12-01 | v2.0 - Fixed deduplication, consolidated docs, repository cleanup |
| 2025-11-28 | v1.5 - Unified ETL script, cron automation |
| 2025-11-27 | v1.0 - Initial production deployment |

---

**License**: Internal use only - SKIDOS
