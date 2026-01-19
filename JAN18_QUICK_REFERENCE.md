# 📋 Jan 18 Quick Reference Card

## 🚨 CRITICAL INFO

**ETL Run Time:** 09:30 UTC (4:00 AM EST / 3:00 PM IST)  
**Monitor Time:** 13:00 UTC (8:00 AM EST / 6:30 PM IST)  
**SSH Command:**
```bash
ssh -i /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/data_analytics_etl.pem ec2-user@44.211.143.180
```

---

## ⚡ INSTANT COMMANDS

### 1. Live Monitoring (During Run)
```bash
tail -f /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log
```

### 2. Quick Status Check
```bash
grep "Successful:" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | tail -1
```

### 3. Count 429 Errors
```bash
grep "429" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log | wc -l
```

### 4. Check Circuit Breaker
```bash
grep "Circuit breaker" /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log
```

### 5. Manual ETL Run
```bash
cd /data/apple-analytics && /home/ec2-user/anaconda3/bin/python3 unified_etl.py
```

### 6. Check Freshness
```bash
cd /data/apple-analytics && /home/ec2-user/anaconda3/bin/python3 monitor_data_freshness.py
```

---

## ✅ SUCCESS INDICATORS

```
✅ "⏱️ Rate limiter: waiting"      - Rate limiting working
✅ "📖 TRUSTED REGISTRY"           - Using cached data
✅ "Successful: 83-92/92"          - Target met
✅ Run completes in 15-25 minutes
✅ Slack report at 13:00 UTC
```

---

## ⚠️ WARNING SIGNS

```
⚠️ "🚨 Rate limited (429)"        - Should be ≤2
⚠️ "🚨 Circuit breaker TRIGGERED" - Should be 0-2
⚠️ "Successful: 50-82/92"         - Partial success
⚠️ Run takes >30 minutes
```

---

## ❌ FAILURE INDICATORS

```
❌ "Successful: <50/92"           - Major failure
❌ Circuit breaker >5 times
❌ Run timeout >60 minutes
❌ No Slack report at 13:00
```

---

## 🔄 EMERGENCY ROLLBACK

```bash
ssh -i /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/data_analytics_etl.pem ec2-user@44.211.143.180

cd /data/apple-analytics/src/extract
cp apple_analytics_client.py.backup_jan17_pre_rate_limit_fix apple_analytics_client.py

cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 unified_etl.py
```

---

## 📊 TARGET METRICS

| Metric | Target | Action if Below |
|--------|--------|-----------------|
| Success Rate | ≥90% | Investigate logs |
| 429 Errors | ≤2 | Check rate limiter |
| Run Time | <30 min | Check for delays |
| Circuit Breaker | ≤2 | Review API quota |

---

## 📱 QUICK CHECKLIST

**09:25 UTC - Pre-Flight**
- [ ] SSH connection works
- [ ] Deployment verified (Jan 17 12:36)

**09:30 UTC - Launch**
- [ ] Slack "Started" notification received
- [ ] Live monitoring started

**10:00 UTC - Mid-Point**
- [ ] ~50 apps processed
- [ ] 429 count ≤2
- [ ] No major errors

**10:15 UTC - Completion**
- [ ] ETL finished
- [ ] Success rate calculated
- [ ] Slack "Success" notification

**13:00 UTC - Report**
- [ ] Monitor ran
- [ ] Slack freshness report
- [ ] Fresh data confirmed

**14:00 UTC - Analysis**
- [ ] Results documented
- [ ] Decision made (keep/adjust/rollback)

---

**Print this and keep it handy! 📋**
