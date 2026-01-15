# Data Freshness Monitoring - Complete Summary

## 🎯 What We Built

**Automated data freshness monitoring with Slack integration** that validates ETL success by checking actual S3 files, not just logs.

---

## ✅ Key Features

### 1. **Comprehensive Data Validation**
- ✅ Checks RAW files (CSV) in S3
- ✅ Checks CURATED files (Parquet) in S3  
- ✅ Monitors registry ages and health
- ✅ Tracks all 5 data types per app
- ✅ Identifies missing data with reasons

### 2. **Automatic Slack Reporting**
- ✅ Rich formatted messages with emoji indicators
- ✅ Smart status detection (✅ HEALTHY / ⚠️ DEGRADED / ❌ CRITICAL)
- ✅ Detailed metrics breakdown
- ✅ Failed apps list with missing data types
- ✅ Registry health tracking

### 3. **Flexible Usage**
- ✅ Command line tool (manual or automated)
- ✅ JSON reports saved to disk
- ✅ Multi-day trend analysis
- ✅ Configurable via environment variables

---

## 📊 How It Works

### Layer 1: Real-time ETL Logs
```
Cron → ETL runs → Logs show per-app results
```
**Use:** During pipeline run, see what's happening

### Layer 2: Data Freshness Check (NEW!)
```
Monitor → Scan S3 → Validate files exist → Report to Slack
```
**Use:** After pipeline run, verify data actually landed in S3

### Layer 3: Multi-day Trends
```
Monitor → Check multiple dates → Show trend → Spot degradation
```
**Use:** Weekly/monthly review, identify patterns

---

## 🚀 Usage Examples

### Daily Automated (Recommended)
```bash
# In crontab:
30 9 * * * /data/apple-analytics/daily_cron.sh
35 9 * * * cd /data/apple-analytics && python3 monitor_data_freshness.py --slack
```
**Result:** Automatic Slack report every day at 09:35 UTC

---

### Manual Check
```bash
# Check yesterday's data
python3 monitor_data_freshness.py

# Check yesterday with Slack notification
python3 monitor_data_freshness.py --slack

# Check specific date
python3 monitor_data_freshness.py --date 2026-01-15

# Check specific date with Slack
python3 monitor_data_freshness.py --date 2026-01-15 --slack
```

---

### Trend Analysis
```bash
# Check last 7 days
python3 monitor_data_freshness.py --days 7

# Check last 30 days
python3 monitor_data_freshness.py --days 30
```

**Output Example:**
```
📈 7-DAY DATA FRESHNESS TREND
✅ 2026-01-16: 85/92 apps (92.4%)
✅ 2026-01-15: 33/92 apps (35.9%)
✅ 2026-01-14: 66/92 apps (71.7%)
✅ 2026-01-13: 74/92 apps (80.4%)
```

---

## 📱 Slack Message Format

### Healthy Pipeline (✅ 80%+ success)
```
✅ Apple Analytics ETL Report - 2026-01-17

Status:          HEALTHY
Success Rate:    75/92 apps (81.5%)

📊 Data Type Coverage:
✅ downloads    85/92 (92.4%)
✅ engagement   88/92 (95.7%)
✅ sessions     87/92 (94.6%)
✅ installs     90/92 (97.8%)
⚠️  purchases    72/92 (78.3%)

✅ Registry Health:
• Average age: 49.3 days
• Oldest: 50 days
• Registries: 75/92

⚠️ Apps Missing Data (17):
• 1596761359 - Missing: downloads, sessions
• 1557847091 - Missing: engagement
...and 12 more apps
```

---

### Degraded Pipeline (⚠️ 50-79% success)
```
⚠️ Apple Analytics ETL Report - 2026-01-15

Status:          DEGRADED
Success Rate:    66/92 apps (71.7%)

📊 Data Type Coverage:
✅ downloads    70/92 (76.1%)
⚠️  engagement   68/92 (73.9%)
⚠️  sessions     65/92 (70.7%)
...
```

---

### Critical Failure (❌ <50% success)
```
❌ Apple Analytics ETL Report - 2026-01-15

Status:          CRITICAL
Success Rate:    33/92 apps (35.9%)

📊 Data Type Coverage:
❌ downloads    35/92 (38.0%)
❌ engagement   33/92 (35.9%)
❌ sessions     30/92 (32.6%)
...

⚠️ ACTION REQUIRED: Check ETL logs immediately!
```

---

## 🔧 Configuration

### Environment Variables (.env)
```bash
# Required
S3_BUCKET=skidos-apptrack
APP_IDS=1506886061,6443744460,1446546237,...

# Optional (for Slack)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Defaults
AWS_REGION=us-east-1
```

### Get Slack Webhook URL
1. Go to: https://api.slack.com/apps
2. Create app → Incoming Webhooks → Enable
3. Add to workspace → Select channel
4. Copy webhook URL
5. Add to `/data/apple-analytics/.env`

---

## 📁 File Structure

```
/data/apple-analytics/
├── monitor_data_freshness.py      ← Main monitoring script
├── .env                            ← Config (SLACK_WEBHOOK_URL here)
├── logs/
│   ├── data_freshness_2026-01-16.json   ← JSON reports
│   ├── data_freshness_2026-01-15.json
│   └── monitor_20260116.log             ← Cron logs
└── unified_etl.py                  ← ETL pipeline
```

---

## 🎯 Success Metrics

### Primary KPIs
| Metric | Target | Alert Threshold |
|--------|--------|----------------|
| Success Rate | ≥ 80% | < 70% |
| downloads | ≥ 90% | < 80% |
| engagement | ≥ 90% | < 80% |
| sessions | ≥ 90% | < 80% |
| installs | ≥ 85% | < 75% |
| purchases | ≥ 70% | < 60% |
| Avg Registry Age | 30-90 days | > 120 days |

### Expected Results (After Fix)
- ✅ **Success Rate**: 81.5% (75/92 apps)
- ✅ **Registry Ages**: 44-50 days (healthy)
- ✅ **Data Coverage**: 90%+ for most types
- ✅ **Failed Apps**: 17 (known issue, requires manual intervention)

---

## 🔍 What Gets Checked

### 1. RAW Data (CSV files)
**Location:** `s3://skidos-apptrack/appstore/raw/{data_type}/dt={processing_date}/app_id={app_id}/`

**Validation:**
- Files exist for processing date?
- Correct number of files (5 per data type)?
- Files have content (not empty)?
- Last modified timestamp reasonable?

---

### 2. CURATED Data (Parquet files)
**Location:** `s3://skidos-apptrack/appstore/curated/{data_type}/dt={metric_date}/app_id={app_id}/data.parquet`

**Validation:**
- Parquet file exists?
- File size > 0 (not empty)?
- All 5 data types present?
- Correct metric date?

---

### 3. Registry Status
**Location:** `s3://skidos-apptrack/analytics_requests/registry/app_id={app_id}/ongoing.json`

**Validation:**
- Registry exists?
- Age within trust period (< 180 days)?
- Request ID present?
- Last verified timestamp?

---

## 📊 Interpretation Guide

### ✅ Healthy
```
Apps with fresh data: 85/92 (92.4%)
downloads: 85/92 (92.4%)
Registry avg age: 49 days
```
**Action:** None, pipeline working well

---

### ⚠️ Warning
```
Apps with fresh data: 66/92 (71.7%)
engagement: 68/92 (73.9%)
Registry avg age: 85 days
```
**Action:** 
- Review ETL logs for errors
- Check failed apps list
- May need intervention soon

---

### ❌ Critical
```
Apps with fresh data: 33/92 (35.9%)
downloads: 35/92 (38.0%)
Many apps failing
```
**Action:**
- **URGENT:** Check ETL logs immediately
- Review recent deployments
- Check for API issues (403, 429, 409)
- May need rollback

---

## 🛠️ Troubleshooting

### Problem: No Slack message received

**Check:**
```bash
# 1. Verify webhook configured
cat /data/apple-analytics/.env | grep SLACK

# 2. Test webhook manually
curl -X POST "$SLACK_WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  -d '{"text":"test"}'

# 3. Check monitor logs
tail -50 /data/apple-analytics/logs/monitor_$(date +%Y%m%d).log
```

---

### Problem: Success rate looks wrong

**Verify:**
```bash
# 1. Check actual S3 files
aws s3 ls s3://skidos-apptrack/appstore/curated/downloads/dt=2026-01-15/ --recursive | wc -l

# 2. Compare to ETL logs
grep "✅ Downloaded" /data/apple-analytics/logs/unified_etl_20260115.log | wc -l

# 3. Re-run monitor
python3 monitor_data_freshness.py --date 2026-01-15
```

---

### Problem: Registry ages increasing

**Check:**
```bash
# View registry distribution
python3 monitor_data_freshness.py | grep -A10 "Age distribution"

# Expected (after fix):
#   0-30d: 0 apps
#   31-60d: 75 apps  ← Most registries here
#   61-90d: 17 apps  ← Apps without registries
#   90+d: 0 apps
```

---

## 📅 Recommended Schedule

### Daily (Automated)
```cron
# ETL runs
30 9 * * * /data/apple-analytics/daily_cron.sh

# Monitor checks and reports to Slack
35 9 * * * cd /data/apple-analytics && python3 monitor_data_freshness.py --slack >> logs/monitor_$(date +\%Y\%m\%d).log 2>&1
```

### Weekly (Manual)
```bash
# Every Monday
python3 monitor_data_freshness.py --days 7
```

### Monthly (Manual)
```bash
# First of month
python3 monitor_data_freshness.py --days 30
```

---

## 📚 Documentation

- `monitor_data_freshness.py` - Main script
- `DATA_FRESHNESS_MONITORING_GUIDE.md` - Detailed usage guide
- `SLACK_INTEGRATION_COMPLETE.md` - Slack setup instructions
- `SLACK_MESSAGE_PREVIEW.md` - Message format examples
- `SLACK_LOGGING_QUICK_REFERENCE.md` - Quick reference

---

## 🎯 Benefits

### Before (Manual Monitoring)
- ❌ Manual log checking (15-20 min/day)
- ❌ No data validation
- ❌ Reactive (find issues after users complain)
- ❌ No historical tracking
- ❌ Time consuming

### After (Automated Monitoring)
- ✅ Automatic Slack reports (0 min/day)
- ✅ S3 data validation
- ✅ Proactive (find issues before users notice)
- ✅ JSON reports for historical analysis
- ✅ Time saved: **60-84 hours/year**

---

## 🔐 Security

- ✅ Never commit `.env` to git
- ✅ Webhook URL is secret (treat like password)
- ✅ Limit Slack channel access
- ✅ Rotate webhook if exposed
- ✅ Use HTTPS for webhook (automatic)

---

## 🚀 Deployment Status

### ✅ Completed
- Enhanced `monitor_data_freshness.py` with Slack integration
- Added `send_to_slack()` method with rich formatting
- Added `format_slack_report()` method
- Added `--slack` command line flag
- Tested compilation (no errors)
- Created comprehensive documentation

### ⬜ To Do (Production)
1. Get Slack webhook URL
2. Add to production `.env`
3. Deploy `monitor_data_freshness.py`
4. Test manually
5. Update crontab
6. Verify first automated report (Jan 17, 09:35 UTC)

---

**Created:** January 16, 2026  
**Status:** ✅ Ready for deployment  
**Next Action:** Get Slack webhook URL and deploy to production
