# Jan 18 Monitoring Checklist

## 🕐 09:30 UTC - ETL Run Start

### Pre-Run Check (09:25 UTC)
```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Verify deployment
ls -lh /data/apple-analytics/src/extract/apple_analytics_client.py
# Should show: Jan 17 12:36 (today's deployment)

# Check cron is ready
crontab -l | grep unified_etl
# Should show: 30 9 * * *
```

---

## 🔍 09:30-10:00 UTC - Active Monitoring

### Watch Live Logs
```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180
tail -f /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log
```

### ✅ Good Signs (What to Look For)
```
⏱️ Rate limiter: waiting 0.XXs for token    # Rate limiting working
📖 TRUSTED REGISTRY | app_id=XXXXX          # Using cached data
✅ Created ONGOING: XXX-XXX-XXX             # New requests succeeding
♻️ Reusing ONGOING request                  # Registry reuse working
```

### ⚠️ Warning Signs (Monitor Count)
```
🚨 Rate limited (429) - Retry-After: XXs    # Should be ≤2 total
⚠️ 403 - trusting registry                  # Should be ≤2 total
🚨 Circuit breaker TRIGGERED                # Should be 0-1
```

### ❌ Red Flags (Trigger Investigation)
```
❌ Rate limited after 3 retries             # Too many 429s
❌ Create ONGOING failed                    # Major failure
ERROR                                       # Any ERROR lines
Exception                                   # Any exceptions
```

---

## 📊 10:00 UTC - Mid-Run Status

### Count Progress
```bash
# Still SSH'd to server
grep "📖 TRUSTED REGISTRY\|✅ Created ONGOING" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | wc -l
# Should be around 45-50 apps by 30 minutes
```

### Check for Issues
```bash
# Count 429 errors
grep "Rate limited (429)" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | wc -l
# ✅ 0-2 = Great
# ⚠️ 3-10 = Monitor closely
# ❌ >10 = Investigate

# Count circuit breaker activations
grep "Circuit breaker TRIGGERED" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | wc -l
# ✅ 0-1 = Great
# ⚠️ 2-3 = Monitor
# ❌ >3 = Problem
```

---

## ✅ 10:15-10:30 UTC - Completion

### Final Success Count
```bash
# Look for final summary in logs
tail -50 /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | grep -A 5 "ETL Summary\|FINAL"
```

### Expected Output
```
✅ Successful: 83-92 apps (90-100%)
⚠️ Failed: 0-9 apps
📊 Total: 92 apps
⏱️ Duration: 15-25 minutes
```

### Success Criteria
- ✅ Success rate ≥90% (83+/92 apps)
- ✅ Run completed in <30 minutes
- ✅ 429 errors ≤2
- ✅ Circuit breaker activations ≤2

---

## 📧 13:00 UTC - Slack Report

### Check Slack Channel
```
Expected message:
📊 Apple Analytics Data Freshness Report
Date: 2026-01-18 13:00 UTC

✅ Data Status: FRESH
📅 Latest Data: 2026-01-17
⏰ Data Age: 0 days old

📱 App Coverage:
- Total Apps: 92
- Apps with Fresh Data: 83-92
- Coverage: 90-100%
```

### If No Report Received
```bash
# Check monitor script logs
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180
cat /data/apple-analytics/logs/monitor_freshness_$(date +%Y%m%d).log

# Run manually if needed
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 monitor_data_freshness.py
```

---

## 📊 14:00 UTC - Final Analysis

### Generate Report
```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Extract key metrics
cd /data/apple-analytics/logs

echo "=== Jan 18 ETL Run Analysis ==="
echo ""

echo "📊 Success Count:"
grep -o "Successful: [0-9]*" unified_etl_$(date +%Y%m%d).log | tail -1

echo ""
echo "🚨 429 Errors:"
grep "Rate limited (429)" unified_etl_$(date +%Y%m%d).log | wc -l

echo ""
echo "⚠️ 403 Errors:"
grep "403 - trusting registry" unified_etl_$(date +%Y%m%d).log | wc -l

echo ""
echo "🔴 Circuit Breaker:"
grep "Circuit breaker" unified_etl_$(date +%Y%m%d).log | wc -l

echo ""
echo "⏱️ Run Duration:"
# Check first and last timestamps
head -1 unified_etl_$(date +%Y%m%d).log
tail -1 unified_etl_$(date +%Y%m%d).log
```

### Decision Matrix

| Success Rate | Action |
|-------------|--------|
| **90-100%** | ✅ **SUCCESS** - Document, monitor for 24h |
| **70-89%** | ⚠️ **PARTIAL** - Keep changes, investigate failures |
| **50-69%** | ⚠️ **CONCERN** - Analyze logs, consider adjustments |
| **<50%** | ❌ **FAILURE** - Rollback immediately |

---

## 🔄 ROLLBACK PROCEDURE

### If Success Rate < 50%

```bash
# Connect to production
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Check current status
cd /data/apple-analytics/src/extract
ls -lh apple_analytics_client.py*

# Rollback
cp apple_analytics_client.py.backup_jan17_pre_rate_limit_fix apple_analytics_client.py

# Verify
/home/ec2-user/anaconda3/bin/python3 -m py_compile apple_analytics_client.py

# Re-run ETL
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 unified_etl.py

# Document rollback reason
echo "ROLLBACK: [describe reason]" >> /data/apple-analytics/logs/rollback_log.txt
```

---

## 📝 Post-Run Documentation

### Create Jan 18 Analysis Doc

```bash
# On local machine
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# Create analysis document
cat > ETL_RUN_ANALYSIS_JAN18.md << 'EOF'
# ETL Run Analysis - January 18, 2026

## Summary
- **Success Rate:** [XX%] ([XX]/92 apps)
- **429 Errors:** [XX]
- **403 Errors:** [XX]
- **Circuit Breaker:** [XX] activations
- **Run Duration:** [XX] minutes

## Comparison to Jan 17
| Metric | Jan 17 | Jan 18 | Change |
|--------|--------|--------|--------|
| Success | 33.7% | [XX%] | [+XX%] |
| 429s | 68 | [XX] | [XX] |
| Time | >60m | [XX]m | [-XX%] |

## Verdict
[✅ SUCCESS / ⚠️ PARTIAL / ❌ FAILURE]

## Next Steps
[Document follow-up actions]
EOF
```

---

## 🎯 Quick Reference

### Critical Commands
```bash
# Connect
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Monitor live
tail -f /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log

# Count successes
grep "Successful:" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log

# Count 429s
grep "429" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | wc -l

# Manual ETL run
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 unified_etl.py

# Rollback
cd /data/apple-analytics/src/extract
cp apple_analytics_client.py.backup_jan17_pre_rate_limit_fix apple_analytics_client.py
```

---

## ✅ Checklist

### 09:25 UTC
- [ ] SSH connection verified
- [ ] Deployment verified (Jan 17 12:36)
- [ ] Cron schedule confirmed

### 09:30-10:00 UTC
- [ ] Live monitoring started
- [ ] Rate limiting messages seen
- [ ] No major errors in first 30 min

### 10:00 UTC
- [ ] Progress check (~45-50 apps done)
- [ ] 429 count ≤2
- [ ] Circuit breaker count ≤1

### 10:15-10:30 UTC
- [ ] ETL completed
- [ ] Success rate calculated
- [ ] Duration recorded

### 13:00 UTC
- [ ] Slack report received
- [ ] Fresh data confirmed
- [ ] App coverage verified

### 14:00 UTC
- [ ] Final analysis completed
- [ ] Decision made (keep/adjust/rollback)
- [ ] Documentation updated

---

**Prepared:** Jan 17, 2026  
**For:** Jan 18, 2026 ETL Run  
**Owner:** [Your Name]
