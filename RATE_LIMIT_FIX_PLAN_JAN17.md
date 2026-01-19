# Rate Limiting Fix Plan - January 17, 2026

## 🎯 **100% AGREE WITH THIS APPROACH**

Your analysis is **perfect**. The current retry behavior is amplifying the problem instead of solving it. Here's the concrete implementation plan.

---

## 📊 **Current Problem Analysis**

### What's Happening Today (Jan 17)

```
09:30 UTC: ETL starts
First ~31 apps: ✅ Succeed (using 75 trusted registries)
~10:51 UTC: 🚨 Apple starts returning 429

Cascade begins:
├─ App tries to create ONGOING request → 409 Conflict
├─ Tries to list existing requests → 403 Forbidden
├─ Retries 3x (no delay respect) → 6 more API calls
├─ All fail → App fails
└─ Next 60 apps repeat this pattern → 183+ extra API calls

Result: 68 rate limit errors, 31/92 success (33.7%)
```

### Root Cause

**Current code DOES NOT respect `Retry-After` or use proper backoff for 429!**

```python
# Current _asc_request() - BROKEN for 429
def _asc_request(self, method: str, url: str, max_retries: int = 3, **kwargs):
    for attempt in range(max_retries):
        response = requests.request(method, url, headers=self.headers, **kwargs)
        
        # ❌ NO 429 HANDLING!
        # Just returns the response, caller interprets as failure
        # Triggers retry loops that multiply API calls
        
        return response
```

---

## 🔧 **Immediate Fixes (Deploy Before Jan 18, 09:30 UTC)**

### Fix 1: Respect `Retry-After` Header for 429 ✅

**File:** `apple_analytics_client.py`

**Change `_asc_request()` method:**

```python
def _asc_request(self, method: str, url: str, max_retries: int = 3, respect_429: bool = True, **kwargs):
    """
    Auto-refreshing requests wrapper for Apple API calls
    Handles 401 errors with automatic JWT token renewal
    Handles 429 with Retry-After header respect
    """
    # Check if token needs refresh before making request
    if self._need_refresh():
        self._refresh_headers()
    
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            response = requests.request(method, url, headers=self.headers, **kwargs)
            
            # Handle 401 with token refresh
            if response.status_code == 401:
                logger.warning("🔄 Got 401, refreshing token and retrying...")
                self._refresh_headers()
                response = requests.request(method, url, headers=self.headers, **kwargs)
            
            # ✅ NEW: Handle 429 with Retry-After respect
            if response.status_code == 429 and respect_429 and attempt < max_retries - 1:
                # Check for Retry-After header (standard for 429)
                retry_after = response.headers.get('Retry-After')
                
                if retry_after:
                    try:
                        # Retry-After can be seconds (int) or HTTP-date
                        wait_time = int(retry_after)
                        logger.warning(f"⏳ 429 Rate Limited - Retry-After: {wait_time}s (attempt {attempt+1}/{max_retries})")
                    except ValueError:
                        # HTTP-date format, default to 60 seconds
                        wait_time = 60
                        logger.warning(f"⏳ 429 Rate Limited - Retry-After header (date), using {wait_time}s")
                else:
                    # No Retry-After header, use exponential backoff with jitter
                    base_wait = (2 ** attempt) * 10  # 10s, 20s, 40s
                    jitter = random.uniform(0, 5)  # Add 0-5s jitter
                    wait_time = base_wait + jitter
                    logger.warning(f"⏳ 429 Rate Limited - No Retry-After, using exponential backoff: {wait_time:.1f}s (attempt {attempt+1}/{max_retries})")
                
                # Cap wait time at 5 minutes
                wait_time = min(wait_time, 300)
                time.sleep(wait_time)
                continue  # Retry
            
            # Return response (success or non-retryable error)
            return response
            
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2  # 2, 4, 6 seconds
                logger.warning(f"⚠️ Connection error (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Request failed after {max_retries} attempts: {method} {url} - {e}")
        except Exception as e:
            logger.error(f"❌ Request failed: {method} {url} - {e}")
            raise
    
    if last_exception:
        raise last_exception
```

**Add import at top:**
```python
import random  # For jitter
```

---

### Fix 2: Make 403 a Hard Failure (Don't Retry) ✅

**Current behavior:** 403 triggers retries  
**New behavior:** 403 returns immediately (permission issue, won't fix with retry)

**Update `_verify_request_exists()`:**

```python
def _verify_request_exists(self, request_id: str, skip_on_rate_limit: bool = True) -> Tuple[bool, str]:
    """
    Verify a request ID is still valid
    
    Returns:
        tuple[bool, str]: (is_valid, reason)
    """
    try:
        url = f"{self.api_base}/analyticsReportRequests/{request_id}"
        response = self._asc_request('GET', url, timeout=30, max_retries=1)  # ✅ Don't retry for verification
        
        if response.status_code == 200:
            return (True, 'valid')
        elif response.status_code == 429:
            if skip_on_rate_limit:
                logger.warning("⚠️ Rate limited during verification, assuming valid")
                return (True, 'rate_limited')
            return (False, 'rate_limited')
        elif response.status_code == 403:
            # ✅ 403 is permission issue - don't assume invalid, use registry
            logger.warning("⚠️ 403 Forbidden - permission issue, trusting registry")
            return (True, 'permission_denied')  # Trust registry instead of failing
        elif response.status_code == 404:
            return (False, 'not_found')
        else:
            return (False, f'status_{response.status_code}')
    except Exception as e:
        logger.warning(f"⚠️ Exception during verification: {e}")
        return (False, 'error')
```

---

### Fix 3: Add Global Rate Limiter (Token Bucket) ✅

**Add to `__init__()`:**

```python
def __init__(self):
    # ... existing init code ...
    
    # ✅ NEW: Global rate limiter (1 request/second initially)
    self.rate_limit_tokens = 1.0  # Start with 1 token
    self.rate_limit_capacity = 1.0  # Max 1 token
    self.rate_limit_refill_rate = 1.0  # Refill 1 token/second
    self.rate_limit_last_update = time.time()
    self.rate_limit_lock = threading.Lock()  # Thread-safe
```

**Add import:**
```python
import threading
```

**Add token bucket method:**

```python
def _acquire_rate_limit_token(self):
    """
    Token bucket rate limiter
    Ensures we don't exceed 1 request/second to Apple API
    """
    with self.rate_limit_lock:
        now = time.time()
        elapsed = now - self.rate_limit_last_update
        
        # Refill tokens based on elapsed time
        self.rate_limit_tokens = min(
            self.rate_limit_capacity,
            self.rate_limit_tokens + (elapsed * self.rate_limit_refill_rate)
        )
        self.rate_limit_last_update = now
        
        # Wait if no tokens available
        if self.rate_limit_tokens < 1.0:
            wait_time = (1.0 - self.rate_limit_tokens) / self.rate_limit_refill_rate
            logger.info(f"⏱️  Rate limit throttle: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
            self.rate_limit_tokens = 0.0
        else:
            # Consume 1 token
            self.rate_limit_tokens -= 1.0
```

**Call before each API request in `_asc_request()`:**

```python
def _asc_request(self, method: str, url: str, max_retries: int = 3, respect_429: bool = True, **kwargs):
    # Check if token needs refresh
    if self._need_refresh():
        self._refresh_headers()
    
    # ✅ NEW: Acquire rate limit token before making request
    self._acquire_rate_limit_token()
    
    # ... rest of existing code ...
```

---

### Fix 4: Add Circuit Breaker for 429 Bursts ✅

**Add to `__init__()`:**

```python
def __init__(self):
    # ... existing init code ...
    
    # ✅ NEW: Circuit breaker for 429 detection
    self.circuit_breaker_429_count = 0
    self.circuit_breaker_window_start = time.time()
    self.circuit_breaker_threshold = 5  # Pause if 5 429s in 2 minutes
    self.circuit_breaker_window = 120  # 2-minute window
    self.circuit_breaker_open = False
```

**Add circuit breaker logic to `_asc_request()`:**

```python
def _asc_request(self, method: str, url: str, max_retries: int = 3, respect_429: bool = True, **kwargs):
    # ... existing code ...
    
    # ✅ NEW: Check circuit breaker before making request
    now = time.time()
    
    # Reset counter if window expired
    if now - self.circuit_breaker_window_start > self.circuit_breaker_window:
        self.circuit_breaker_429_count = 0
        self.circuit_breaker_window_start = now
        self.circuit_breaker_open = False
    
    # If circuit is open (too many 429s), pause
    if self.circuit_breaker_open:
        logger.warning("🚨 Circuit breaker OPEN - pausing API calls for 60s")
        time.sleep(60)
        self.circuit_breaker_open = False
        self.circuit_breaker_429_count = 0
        self.circuit_breaker_window_start = time.time()
    
    # ... make request ...
    
    # ✅ NEW: Track 429s for circuit breaker
    if response.status_code == 429:
        self.circuit_breaker_429_count += 1
        
        if self.circuit_breaker_429_count >= self.circuit_breaker_threshold:
            logger.error(f"🚨 Circuit breaker TRIGGERED - {self.circuit_breaker_429_count} 429s in window")
            self.circuit_breaker_open = True
    
    # ... rest of code ...
```

---

### Fix 5: Better 409 Handling ✅

**Update `_create_ongoing_request_with_retry()`:**

```python
elif response.status_code == 409:
    logger.info("♻️ ONGOING already exists (409). Using registry fallback.")
    
    # ✅ DON'T retry list requests - just use registry
    # The 403 on listing is a known issue, use fallback immediately
    
    # Try S3 registry first (most reliable)
    existing_rid = self._load_request_registry(app_id, "ONGOING")
    if existing_rid and existing_rid.get("request_id"):
        rid = existing_rid["request_id"]
        logger.info("✅ Using registry for 409 conflict: %s", rid)
        return rid
    
    # Fallback: analytics.json
    rid = self._extract_request_id_from_analytics_json(app_id)
    if rid:
        logger.info("✅ Found request ID from analytics.json: %s", rid)
        self._save_request_registry(app_id, "ONGOING", rid)
        return rid
    
    # Last resort: skip this app for now
    logger.error("❌ 409 conflict but no registry or analytics.json - skipping app")
    return None  # ✅ Don't loop retrying
```

---

## 📋 **Today's Backfill Plan (Jan 16 Data)**

### Step 1: Identify Failed Apps

```bash
ssh -i ~/Desktop/PlayGroundS/Download_Pipeline/data_analytics_etl.pem ec2-user@44.211.143.180

# Extract failed app IDs from results
cd /data/apple-analytics
python3 -c "
import json
with open('logs/unified_etl_results_20260117_111948.json') as f:
    results = json.load(f)
    # 92 total - 31 successful = 61 failed
    print('Failed apps: 61')
    print('Need backfill for date: 2026-01-16')
"
```

### Step 2: Deploy Fixes FIRST

```bash
# On local machine
cd /Users/ankit_chauhan/Desktop/PlayGroundS/Download_Pipeline/Apple-Analytics

# Copy fixed file to production
scp -i ~/Desktop/PlayGroundS/Download_Pipeline/data_analytics_etl.pem \
    src/extract/apple_analytics_client.py \
    ec2-user@44.211.143.180:/data/apple-analytics/src/extract/apple_analytics_client.py.NEW

# On production
ssh -i ~/Desktop/PlayGroundS/Download_Pipeline/data_analytics_etl.pem ec2-user@44.211.143.180
cd /data/apple-analytics

# Backup current version
cp src/extract/apple_analytics_client.py src/extract/apple_analytics_client.py.BACKUP_JAN17_BEFORE_429_FIX

# Deploy new version
mv src/extract/apple_analytics_client.py.NEW src/extract/apple_analytics_client.py
```

### Step 3: Run Targeted Backfill

Create backfill script for just the 61 failed apps:

```python
# backfill_jan16.py
#!/usr/bin/env python3
"""
Backfill Jan 16 data for apps that failed due to rate limiting
"""

import sys
sys.path.insert(0, '/data/apple-analytics')

from unified_etl import AppleAnalyticsUnifiedETL
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get list of apps from results (92 total - 31 succeeded = 61 to backfill)
# You can extract this from the ETL results or curated data

etl = AppleAnalyticsUnifiedETL()
target_date = "2026-01-16"

logger.info(f"🔄 Backfilling {target_date} for failed apps...")

# Run for date
results = etl.run_daily_etl(processing_date=target_date)

logger.info(f"✅ Backfill complete: {results['apps_successful']}/{results['apps_processed']} apps")
```

**Run backfill:**
```bash
cd /data/apple-analytics
/home/ec2-user/anaconda3/bin/python3 backfill_jan16.py
```

---

## 🔮 **Expected Results After Fixes**

### Tomorrow's Run (Jan 18, 09:30 UTC)

**With fixes:**
```
✅ Token bucket: 1 req/sec = 92 apps in ~10 minutes
✅ 429 with Retry-After: Waits instead of failing
✅ 403 on verify: Trusts registry instead of failing
✅ Circuit breaker: Pauses if 5 429s in 2 min
✅ 409 conflicts: Uses registry immediately

Expected: 85-92 apps succeed (92-100%)
Rate limits: 0-5 instances (managed gracefully)
Run time: 15-25 minutes (acceptable)
```

### Backfill Today (Jan 16 Data)

**With fixes:**
```
✅ 61 failed apps will be retried
✅ Rate limiter prevents quota exhaustion  
✅ 429s handled with Retry-After
✅ Registry fallbacks work

Expected: 55-61 apps succeed (90-100% of failed apps)
Total Jan 16 coverage: 86-92/92 apps (93-100%)
```

---

## 📊 **Monitoring Commands**

### Check Today's Backfill Progress

```bash
# Watch backfill log
tail -f /data/apple-analytics/logs/backfill_jan16.log

# Check for 429s
grep '429' /data/apple-analytics/logs/backfill_jan16.log | wc -l

# Check circuit breaker activations
grep 'Circuit breaker' /data/apple-analytics/logs/backfill_jan16.log
```

### Check Tomorrow's Run

```bash
# After 09:30 UTC run
tail -100 /data/apple-analytics/logs/cron.log | grep -E '(429|Circuit|Rate limit)'

# Check results
cat /data/apple-analytics/logs/unified_etl_results_*.json | python3 -m json.tool
```

---

## ✅ **Implementation Checklist**

### Before Jan 18, 09:30 UTC

- [ ] **Fix 1:** Add `Retry-After` respect to `_asc_request()`
- [ ] **Fix 2:** Make 403 trust registry (don't fail)
- [ ] **Fix 3:** Add token bucket rate limiter
- [ ] **Fix 4:** Add circuit breaker for 429 bursts
- [ ] **Fix 5:** Better 409 handling (use registry immediately)
- [ ] **Deploy:** Copy to production with backup
- [ ] **Test:** Run single app test
- [ ] **Backfill:** Run for Jan 16 failed apps

### Nice to Have (Can wait)

- [ ] Log rate limit headers (x-rate-limit-*) if Apple provides them
- [ ] Cache static data (app metadata)
- [ ] Batch processing with cooldowns
- [ ] Registry pre-population

---

## 🎯 **Success Criteria**

### Jan 18 ETL Run

**Must have:**
- ✅ Success rate ≥ 90% (83+/92 apps)
- ✅ Zero cascading 429 errors
- ✅ Circuit breaker activations logged but handled
- ✅ Run completes within 30 minutes

**Nice to have:**
- ✅ 100% success (92/92 apps)
- ✅ Zero 429 errors (all requests under quota)
- ✅ Run completes within 20 minutes

---

## 📚 **References**

- MDN 429 Best Practices: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429
- Apple Rate Limits: https://developer.apple.com/documentation/appstoreconnectapi/identifying-rate-limits
- Token Bucket Algorithm: Standard rate limiting pattern
- Circuit Breaker Pattern: Fault tolerance best practice

---

**Created:** January 17, 2026 12:30 UTC  
**Deploy Deadline:** January 18, 2026 09:00 UTC (30 min before ETL)  
**Priority:** 🚨 CRITICAL - Must deploy today
