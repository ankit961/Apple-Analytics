# Slack Integration - Complete Setup Guide

## 🎯 Overview

The data freshness monitor now includes **built-in Slack reporting** that automatically sends comprehensive reports after each ETL run. This eliminates the need for manual log checking and provides instant visibility into pipeline health.

---

## ✅ What's Included

### 1. **Automatic Slack Notifications**
- Rich formatted reports with emoji indicators
- Success rate with visual status (✅/⚠️/❌)
- Data type breakdown per metric
- Registry health tracking
- Failed apps list with details
- Automatic timestamp

### 2. **Smart Status Detection**
- ✅ **HEALTHY** (≥80% success rate)
- ⚠️ **DEGRADED** (50-79% success rate)  
- ❌ **CRITICAL** (<50% success rate)

### 3. **Comprehensive Metrics**
- Success rate (apps with fresh data)
- Processing & metric dates
- Data type coverage (downloads, engagement, etc.)
- Registry age distribution
- Apps missing data with reasons

---

## 🚀 Quick Start

### Step 1: Get Slack Webhook URL

1. **Create Slack Incoming Webhook:**
   - Go to: https://api.slack.com/apps
   - Click "Create New App" → "From scratch"
   - Name: "Apple Analytics Monitor"
   - Select your workspace
   - Click "Incoming Webhooks" → Toggle ON
   - Click "Add New Webhook to Workspace"
   - Select channel (e.g., #apple-analytics-alerts)
   - Copy the webhook URL

2. **Add to .env file:**
```bash
# Add this line to /data/apple-analytics/.env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR_WORKSPACE_ID/YOUR_CHANNEL_ID/YOUR_SECRET_TOKEN
```

---

### Step 2: Test Slack Integration (Local)

```bash
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# Add webhook to local .env
echo 'SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL' >> .env

# Test with yesterday's data
python3 monitor_data_freshness.py --slack

# Test with specific date
python3 monitor_data_freshness.py --date 2026-01-15 --slack
```

**Expected Output:**
```
================================================================================
📊 DATA FRESHNESS REPORT - 2026-01-16
================================================================================
...
📄 Report saved to: logs/data_freshness_2026-01-16.json
✅ Slack notification sent successfully
```

---

### Step 3: Deploy to Production

```bash
# From local machine
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# Copy updated monitor script
scp monitor_data_freshness.py ec2-user@<PRODUCTION_IP>:/data/apple-analytics/

# SSH to production
ssh -i <PATH_TO_PEM_FILE> ec2-user@<PRODUCTION_IP>

# Add webhook to .env
cd /data/apple-analytics
echo 'SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL' >> .env

# Test on production
python3 monitor_data_freshness.py --date 2026-01-15 --slack
```

---

### Step 4: Update Cron Job

```bash
# SSH to production
ssh -i <PATH_TO_PEM_FILE> ec2-user@<PRODUCTION_IP>

# Edit cron
crontab -e

# Update to include Slack reporting after ETL
# OLD:
30 9 * * * /data/apple-analytics/daily_cron.sh

# NEW:
30 9 * * * /data/apple-analytics/daily_cron.sh
35 9 * * * cd /data/apple-analytics && python3 monitor_data_freshness.py --slack >> logs/monitor_$(date +\%Y\%m\%d).log 2>&1
```

**Explanation:**
- **09:30 UTC**: ETL runs
- **09:35 UTC**: Monitor checks data and sends Slack report (5 min buffer for ETL to complete)

---

## 📊 Slack Message Format

### ✅ Healthy Pipeline (80%+ success)

```
✅ Apple Analytics ETL Report - 2026-01-17
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status:             HEALTHY
Success Rate:       75/92 apps (81.5%)
Processing Date:    2026-01-17
Metric Date:        2026-01-16

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Data Type Coverage:
✅ downloads      85/92 (92.4%)
✅ engagement     88/92 (95.7%)
✅ sessions       87/92 (94.6%)
✅ installs       90/92 (97.8%)
⚠️  purchases      72/92 (78.3%)

✅ Registry Health:
• Average age: 49.3 days
• Oldest: 50 days
• Registries: 75/92

⚠️ Apps Missing Data (17):
• 1596761359 - Missing: downloads, sessions
• 1557847091 - Missing: engagement
• 1446546237 - Missing: purchases
• 6443744460 - Missing: installs, purchases
• 1539453716 - Missing: downloads
...and 12 more apps

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generated at 2026-01-17 09:36:15 UTC
```

---

### ⚠️ Degraded Pipeline (50-79% success)

```
⚠️ Apple Analytics ETL Report - 2026-01-15
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status:             DEGRADED
Success Rate:       66/92 apps (71.7%)
Processing Date:    2026-01-15
Metric Date:        2026-01-14

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Data Type Coverage:
✅ downloads      70/92 (76.1%)
⚠️  engagement     68/92 (73.9%)
⚠️  sessions       65/92 (70.7%)
✅ installs       72/92 (78.3%)
⚠️  purchases      60/92 (65.2%)

⚠️ Registry Health:
• Average age: 58.2 days
• Oldest: 60 days
• Registries: 68/92

⚠️ Apps Missing Data (26):
• [List of apps...]
```

---

### ❌ Critical Failure (<50% success)

```
❌ Apple Analytics ETL Report - 2026-01-15
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Status:             CRITICAL
Success Rate:       33/92 apps (35.9%)
Processing Date:    2026-01-15
Metric Date:        2026-01-14

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 Data Type Coverage:
❌ downloads      35/92 (38.0%)
❌ engagement     33/92 (35.9%)
❌ sessions       30/92 (32.6%)
❌ installs       38/92 (41.3%)
❌ purchases      28/92 (30.4%)

❌ Registry Health:
• Average age: 48.5 days
• Oldest: 50 days
• Registries: 75/92

⚠️ Apps Missing Data (59):
• [First 5 apps listed...]
...and 54 more apps

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ ACTION REQUIRED: Check ETL logs immediately!
```

---

## 🔧 Configuration Options

### Environment Variables

```bash
# Required
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
S3_BUCKET=skidos-apptrack
APP_IDS=1506886061,6443744460,1446546237,...

# Optional (with defaults)
AWS_REGION=us-east-1
```

### Command Line Options

```bash
# Basic usage (yesterday's data, no Slack)
python3 monitor_data_freshness.py

# Send to Slack
python3 monitor_data_freshness.py --slack

# Specific date with Slack
python3 monitor_data_freshness.py --date 2026-01-15 --slack

# Multi-day trend (no Slack support yet)
python3 monitor_data_freshness.py --days 7
```

---

## 📋 Manual Testing

### Test 1: Verify Webhook Works

```bash
cd /data/apple-analytics

# Test with curl
curl -X POST "${SLACK_WEBHOOK_URL}" \
  -H 'Content-Type: application/json' \
  -d '{"text":"✅ Test message from Apple Analytics Monitor"}'
```

**Expected:** Message appears in Slack channel

---

### Test 2: Test Monitor Script

```bash
cd /data/apple-analytics

# Run without Slack (dry run)
python3 monitor_data_freshness.py --date 2026-01-15

# Run with Slack
python3 monitor_data_freshness.py --date 2026-01-15 --slack
```

**Expected:** 
- Console output shows report
- Slack message appears in channel
- `✅ Slack notification sent successfully`

---

### Test 3: Check Saved Reports

```bash
cd /data/apple-analytics

# View saved JSON report
cat logs/data_freshness_2026-01-15.json | jq '.'

# Check report structure
cat logs/data_freshness_2026-01-15.json | jq '.apps_with_fresh_curated_data, .total_apps'
```

---

## 🔍 Troubleshooting

### Problem: "SLACK_WEBHOOK_URL not configured"

**Solution:**
```bash
# Check .env file
cat /data/apple-analytics/.env | grep SLACK

# Add if missing
echo 'SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL' >> /data/apple-analytics/.env
```

---

### Problem: "Failed to send Slack notification"

**Possible Causes:**
1. **Invalid webhook URL** - Check URL is correct
2. **Network issue** - Test with curl
3. **Slack app disabled** - Check Slack app settings

**Debug:**
```bash
# Test webhook with curl
curl -X POST "${SLACK_WEBHOOK_URL}" -H 'Content-Type: application/json' -d '{"text":"test"}'

# Check Python requests library
python3 -c "import requests; print(requests.__version__)"
```

---

### Problem: Message sent but formatting looks wrong

**Solution:**
- Slack blocks have character limits
- Very long app lists are truncated to first 5
- Check Slack Block Kit for formatting rules: https://api.slack.com/block-kit

---

## 📅 Recommended Schedule

### Daily Automated Reports

```cron
# ETL runs at 09:30 UTC
30 9 * * * /data/apple-analytics/daily_cron.sh

# Monitor checks and reports at 09:35 UTC (5 min buffer)
35 9 * * * cd /data/apple-analytics && python3 monitor_data_freshness.py --slack >> logs/monitor_$(date +\%Y\%m\%d).log 2>&1
```

### Weekly Manual Review

```bash
# Every Monday, check 7-day trend
python3 monitor_data_freshness.py --days 7
```

### Monthly Audit

```bash
# First of month, check 30-day trend
python3 monitor_data_freshness.py --days 30
```

---

## 🎯 Success Criteria

After deployment, you should see:

✅ **Daily Slack Messages** (every day at ~09:35 UTC)  
✅ **Success Rate ≥ 80%** (75+/92 apps)  
✅ **Data Type Coverage ≥ 90%** (downloads, engagement, sessions)  
✅ **Registry Age < 90 days** (average)  
✅ **Consistent Reporting** (no missed days)  

---

## 📚 Related Files

- `monitor_data_freshness.py` - Main monitoring script with Slack integration
- `DATA_FRESHNESS_MONITORING_GUIDE.md` - Detailed monitoring guide
- `SLACK_MESSAGE_PREVIEW.md` - Message format examples
- `SLACK_LOGGING_QUICK_REFERENCE.md` - Quick reference
- `.env` - Configuration file (not in git)

---

## 🔐 Security Notes

1. **Never commit .env to git** - Contains sensitive webhook URL
2. **Webhook URL is secret** - Treat like a password
3. **Rotate webhook if exposed** - Regenerate in Slack app settings
4. **Limit channel access** - Only authorized users in Slack channel

---

## 📞 Next Steps

1. ✅ **Get Slack webhook URL** (from Slack admin)
2. ✅ **Add to production .env**
3. ✅ **Deploy monitor script**
4. ✅ **Test manually**
5. ✅ **Update cron job**
6. ✅ **Wait for next ETL run** (Jan 17, 09:30 UTC)
7. ✅ **Verify Slack report** (Jan 17, 09:35 UTC)

---

**Created:** January 16, 2026  
**Status:** Ready for deployment ✅  
**Next Action:** Get Slack webhook URL and deploy
