# Rate Limiting Fixes Implementation - Jan 17, 2026

## 🚨 CRITICAL DEPLOYMENT FOR JAN 18 ETL RUN

**Deadline:** Deploy by **09:00 UTC Jan 18** (before ETL runs at 09:30 UTC)

---

## ✅ IMPLEMENTED FIXES

### 1. **Token Bucket Rate Limiter** ✅
**Location:** `_acquire_rate_limit_token()` method

**Implementation:**
- 1 token per second capacity (1 req/sec global rate)
- Thread-safe with `threading.Lock()`
- Automatically refills tokens over time
- Blocks requests when no tokens available

**Impact:** Prevents hitting Apple API quota limits proactively

---

### 2. **Retry-After Header Respect** ✅
**Location:** `_asc_request()` method

**Implementation:**
```python
# Handle 429 rate limiting with Retry-After header
if response.status_code == 429:
    self._record_429_error()
    
    # Check Retry-After header
    retry_after = response.headers.get('Retry-After')
    if retry_after:
        wait_time = int(retry_after)
    else:
        # Exponential backoff with jitter: 10s, 20s, 40s + random 0-5s
        wait_time = (2 ** attempt) * 10 + random.uniform(0, 5)
```

**Impact:** 
- Respects Apple's rate limit guidance
- Reduces cascading 429 errors from 68 → near zero
- Better backoff strategy (10s, 20s, 40s vs old 5s, 10s, 20s)

---

### 3. **Circuit Breaker** ✅
**Location:** `_check_circuit_breaker()` and `_record_429_error()` methods

**Implementation:**
- Tracks 429 errors in 2-minute sliding window
- Opens circuit after 5x 429s in 2 minutes
- Forces pause until window expires
- Auto-resets after cooling period

**Logic:**
```python
def _check_circuit_breaker(self):
    """Check if too many 429s recently - if so, pause"""
    if self.circuit_breaker_open:
        time_remaining = self.circuit_breaker_window - (now - window_start)
        logger.warning(f"🚨 Circuit breaker OPEN - pausing {time_remaining:.1f}s")
        time.sleep(time_remaining)
        # Reset after pause
        self.circuit_breaker_open = False
```

**Impact:** 
- Prevents API quota exhaustion
- Gives Apple API time to recover
- Saves remaining apps from cascading failures

---

### 4. **403 Hard Failure (Trust Registry)** ✅
**Location:** `_verify_request_exists()` method

**Changes:**
- `max_retries=1` for verification (don't waste quota)
- 403 returns `(True, 'permission_denied')` instead of `(False, ...)`
- Registry is trusted on 403 errors

**Old behavior:**
```python
elif response.status_code == 403:
    return (False, 'forbidden')  # Caused re-creation attempts
```

**New behavior:**
```python
elif response.status_code == 403:
    logger.warning("⚠️ 403 - trusting registry (API permission issue)")
    return (True, 'permission_denied')  # Trust registry, don't retry
```

**Impact:** Eliminates 2+ API calls per 403 error (retries + re-verification)

---

### 5. **Improved 409 Handling** ✅
**Location:** `_create_ongoing_request_with_retry()` method

**Changes:**
- On 409: Check registry FIRST (fastest, no API calls)
- Then analytics.json fallback
- API listing only as last resort
- No retry loops that waste quota

**Old flow:**
1. 409 → retry API list (often fails with 403)
2. Retry API list again
3. Retry API list again
4. Finally try analytics.json

**New flow:**
1. 409 → check registry (instant, no API call)
2. If no registry → analytics.json (no API call)
3. If still nothing → try API list once

**Impact:** Reduces API calls on 409 from ~3-6 → 0-1

---

## 📊 EXPECTED RESULTS

### Before Fixes (Jan 17)
| Metric | Value |
|--------|-------|
| Success Rate | 33.7% (31/92) |
| 429 Errors | 68 |
| 403 Errors | 12 |
| 409 Conflicts | 18 |
| Wasted API Calls | 180+ (retries) |

### After Fixes (Jan 18 Target)
| Metric | Target |
|--------|--------|
| Success Rate | **≥90%** (83+/92) |
| 429 Errors | **0-2** (circuit breaker prevents cascade) |
| 403 Errors | **0-2** (trust registry, don't retry) |
| 409 Conflicts | **0-5** (registry use, fast fallback) |
| Wasted API Calls | **<20** (no retry loops) |

---

## 🚀 DEPLOYMENT STEPS

### 1. **Test Locally** (5 min)
```bash
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# Quick syntax check
python3 -m py_compile src/extract/apple_analytics_client.py

# Test with single app (optional)
# python3 -c "from src.extract.apple_analytics_client import AppleAnalyticsRequestor; r = AppleAnalyticsRequestor(); print('✅ Import successful')"
```

### 2. **Backup Production** (2 min)
```bash
ssh -i /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics/data_analytics_etl.pem ec2-user@44.211.143.180

# Create timestamped backup
cd /data/apple-analytics/src/extract
cp apple_analytics_client.py apple_analytics_client.py.backup_jan17_pre_rate_limit_fix
```

### 3. **Deploy to Production** (3 min)
```bash
# From local machine
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

scp -i data_analytics_etl.pem \
  src/extract/apple_analytics_client.py \
  ec2-user@44.211.143.180:/data/apple-analytics/src/extract/
```

### 4. **Verify Deployment** (2 min)
```bash
ssh -i /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics/data_analytics_etl.pem ec2-user@44.211.143.180

# Check file updated
ls -la /data/apple-analytics/src/extract/apple_analytics_client.py

# Check for syntax errors
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 -m py_compile src/extract/apple_analytics_client.py

# Quick import test
/home/ec2-user/anaconda3/bin/python3 -c "from src.extract.apple_analytics_client import AppleAnalyticsRequestor; print('✅ Production deployment verified')"
```

---

## 📋 POST-DEPLOYMENT MONITORING

### Jan 18, 09:30 UTC - ETL Run
**Monitor for:**
- ✅ Run completes in <30 minutes (vs Jan 17's timeout)
- ✅ Success rate ≥90% (83+/92 apps)
- ✅ Zero circuit breaker activations (or 1-2 max)
- ✅ 429 errors ≤2
- ✅ No retry loops in logs

### Jan 18, 13:00 UTC - Slack Report
**Verify automated report shows:**
- ✅ Fresh data for Jan 17 (yesterday)
- ✅ 90-100% app coverage
- ✅ No stale data warnings

**Check logs:**
```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Check ETL logs
tail -100 /data/apple-analytics/logs/unified_etl_$(date +%Y%m%d).log

# Look for:
# - "🚨 Circuit breaker" messages (should be 0)
# - "Rate limited (429)" messages (should be ≤2)
# - "⏱️ Rate limiter: waiting" messages (normal, shows rate limiting working)
# - Final success count (should be 83-92/92)
```

---

## 🔄 ROLLBACK PLAN (if needed)

If Jan 18 run fails worse than Jan 17:

```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

cd /data/apple-analytics/src/extract

# Restore previous version
cp apple_analytics_client.py.backup_jan17_pre_rate_limit_fix apple_analytics_client.py

# Verify rollback
python3 -m py_compile apple_analytics_client.py

# Re-run ETL manually
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 unified_etl.py
```

---

## 📝 CODE CHANGES SUMMARY

### Added Methods
1. `_acquire_rate_limit_token()` - Token bucket rate limiter
2. `_check_circuit_breaker()` - Check if too many 429s
3. `_record_429_error()` - Track 429s for circuit breaker

### Modified Methods
1. `_asc_request()` - Added Retry-After respect + circuit breaker check
2. `_verify_request_exists()` - 403 returns True (trust registry)
3. `_create_ongoing_request_with_retry()` - Registry-first 409 handling

### Added Imports
```python
import random  # For jitter in exponential backoff
import threading  # For thread-safe rate limiting
```

### Added Instance Variables
```python
# Token bucket rate limiter
self.rate_limit_tokens = 1.0
self.rate_limit_capacity = 1.0
self.rate_limit_refill_rate = 1.0
self.rate_limit_last_update = time.time()
self.rate_limit_lock = threading.Lock()

# Circuit breaker
self.circuit_breaker_429_count = 0
self.circuit_breaker_window_start = time.time()
self.circuit_breaker_threshold = 5
self.circuit_breaker_window = 120
self.circuit_breaker_open = False
```

---

## ✅ READINESS CHECKLIST

- [x] Token bucket rate limiter implemented
- [x] Retry-After header respect added
- [x] Circuit breaker implemented
- [x] 403 hard failure (trust registry) added
- [x] 409 registry-first handling updated
- [x] Imports added (random, threading)
- [x] Instance variables initialized
- [ ] Local syntax check passed
- [ ] Production backup created
- [ ] Deployed to production
- [ ] Deployment verified
- [ ] Monitoring plan ready

---

## 🎯 SUCCESS CRITERIA

### Must Have (Critical)
- ✅ Success rate ≥90% (83+/92 apps)
- ✅ Zero cascading 429 errors
- ✅ ETL completes in <30 minutes

### Should Have (High Priority)
- ✅ Circuit breaker activations ≤2
- ✅ Fresh data for Jan 17
- ✅ Automated Slack report delivers

### Nice to Have
- ✅ 95%+ success rate (87+/92)
- ✅ Zero circuit breaker activations
- ✅ Run completes in <20 minutes

---

## 📞 ESCALATION

**If deployment fails:**
1. Check syntax errors with `python3 -m py_compile`
2. Review error logs in `/data/apple-analytics/logs/`
3. Rollback to previous version
4. Document failure reason

**If Jan 18 run fails:**
1. Analyze logs for new error patterns
2. Check if circuit breaker activated (look for "🚨 Circuit breaker")
3. Verify rate limiting is working (look for "⏱️ Rate limiter")
4. Consider manual backfill for failed apps

---

**Deployed by:** [Your Name]
**Deployment Date:** Jan 17, 2026 (before 23:59 UTC)
**Verification Date:** Jan 18, 2026 (09:30 - 14:00 UTC)
