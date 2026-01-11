# Apple Analytics ETL - Rate Limiting Fixes Implementation
**Date:** 2026-01-11  
**Status:** ✅ IMPLEMENTED

---

## 🎯 CHANGES IMPLEMENTED

### 1. Enhanced Registry Trust System

**File:** `apple_analytics_client.py`

#### Added `_should_trust_registry()` Method
```python
def _should_trust_registry(self, registry_data: Dict, max_age_days: int = 30) -> bool:
```

**Purpose:** Trust ONGOING requests from registry without verification if created within 30 days.

**Benefits:**
- ✅ Avoids unnecessary API calls for recent requests
- ✅ ONGOING requests don't expire, safe to trust
- ✅ Reduces API calls by ~92 per run

---

### 2. Improved `_verify_request_exists()` - Proper 429 Handling

**Before:**
```python
def _verify_request_exists(self, request_id: str) -> bool:
    try:
        response = self._asc_request('GET', url, timeout=30)
        return response.status_code == 200
    except Exception:
        return False  # ❌ TREATS 429 AS INVALID!
```

**After:**
```python
def _verify_request_exists(self, request_id: str, skip_on_rate_limit: bool = True) -> tuple[bool, str]:
    try:
        response = self._asc_request('GET', url, timeout=30)
        
        if response.status_code == 200:
            return (True, 'valid')
        elif response.status_code == 429:
            # ✅ Don't treat rate limits as invalid!
            if skip_on_rate_limit:
                logger.warning("⚠️ Rate limited during verification, assuming valid")
                return (True, 'rate_limited')  # Assume valid to avoid cascade
            return (False, 'rate_limited')
        elif response.status_code == 404:
            return (False, 'not_found')
        else:
            return (False, f'status_{response.status_code}')
    except Exception as e:
        return (False, 'error')
```

**Benefits:**
- ✅ Distinguishes between "invalid" and "rate limited"
- ✅ Prevents false positives that trigger cascading failures
- ✅ Returns detailed reason codes for debugging

---

### 3. Exponential Backoff Retry Methods

#### Added `_find_existing_ongoing_request_with_retry()`
```python
def _find_existing_ongoing_request_with_retry(self, app_id: str, max_retries: int = 3) -> Optional[str]:
```

**Retry Strategy:**
- Attempt 1: Immediate
- Attempt 2: Wait 5 seconds
- Attempt 3: Wait 10 seconds
- Attempt 4: Wait 20 seconds

**Benefits:**
- ✅ Handles transient 429 errors gracefully
- ✅ Exponential backoff: `wait_time = (2 ** attempt) * 5`
- ✅ Max 3 retries to avoid infinite loops

#### Added `_create_ongoing_request_with_retry()`
```python
def _create_ongoing_request_with_retry(self, app_id: str, max_retries: int = 3) -> Optional[str]:
```

**Features:**
- ✅ Same exponential backoff strategy
- ✅ Handles 409 conflicts (already exists)
- ✅ Proper error logging with attempt numbers

---

### 4. Updated `create_or_reuse_ongoing_request()` Logic

**Optimization Flow:**

```
1. Load registry
   ↓
2. Is registry < 30 days old?
   YES → Trust it, skip verification ✅
   NO  → Continue to verification
   ↓
3. Verify request exists (with 429 handling)
   VALID → Return request ID ✅
   RATE_LIMITED → Trust registry anyway ✅
   NOT_FOUND → Continue to find/create
   ↓
4. Find existing via API (with retry)
   FOUND → Save to registry, return ✅
   NOT_FOUND → Continue to create
   ↓
5. Create new request (with retry)
   SUCCESS → Save to registry, return ✅
   FAILED → Return None ❌
```

**Key Changes:**
- ✅ Trust recent registry entries (< 30 days) without verification
- ✅ Handle 429 during verification gracefully (don't invalidate)
- ✅ Use retry methods with exponential backoff
- ✅ Multiple fallback paths to maximize success rate

---

### 5. Inter-App Delay (10 Seconds)

**File:** `unified_etl.py`

**Before:**
```python
for aid in app_ids:
    result = self.extract_app_data(aid, date_str)
```

**After:**
```python
for idx, aid in enumerate(app_ids):
    # Add delay between apps to avoid rate limiting (except for first app)
    if idx > 0:
        logger.info(f"⏱️  Waiting 10 seconds before processing next app...")
        time.sleep(10)
    
    result = self.extract_app_data(aid, date_str)
```

**Benefits:**
- ✅ Prevents burst API requests
- ✅ Gives Apple's rate limiter time to reset
- ✅ 10-second delay = ~15 minutes total for 92 apps (acceptable)

---

### 6. Updated `_load_request_registry()` - Returns Full Metadata

**Before:**
```python
def _load_request_registry(...) -> Optional[str]:
    # Returned just request_id
    return rid
```

**After:**
```python
def _load_request_registry(...) -> Optional[Dict]:
    # Returns full metadata including created_at
    return data  # {'request_id': '...', 'created_at': '...', 'access_type': '...'}
```

**Benefits:**
- ✅ Enables age-based trust decisions
- ✅ Better audit trail
- ✅ Supports future enhancements

---

## 📊 EXPECTED IMPACT

### Before Fixes:
| Metric | Value | Issue |
|--------|-------|-------|
| Apps Processed | 92 | ✓ All attempted |
| Apps Successful | 27 (29%) | ❌ Most failed |
| Apps Failed | 65 (71%) | ❌ Rate limit cascade |
| API Calls | Up to 276 | ❌ Too many |
| Run Time | ~5 minutes | ✓ Fast but fails |

**Failure Pattern:**
- First 20-30 apps succeed
- Remaining 65 apps hit cascading 429 errors
- Each failed app tries 3 API calls (verify → find → create)

### After Fixes:
| Metric | Expected Value | Improvement |
|--------|---------------|-------------|
| Apps Processed | 92 | Same |
| Apps Successful | 92 (100%) | ✅ +241% |
| Apps Failed | 0 (0%) | ✅ Fixed |
| API Calls | ~92 | ✅ -67% reduction |
| Run Time | ~20 minutes | Acceptable tradeoff |

**Success Pattern:**
- All 92 apps succeed
- Registry trust skips 92 verification calls
- 10-second delays prevent bursts
- Retries handle transient 429s

---

## 🔧 DEPLOYMENT INSTRUCTIONS

### 1. Upload Updated Files to EC2

```bash
# From local machine
scp -i data_analytics_etl.pem \
  Apple-Analytics/src/extract/apple_analytics_client.py \
  ec2-user@44.211.143.180:/data/apple-analytics/src/extract/

scp -i data_analytics_etl.pem \
  Apple-Analytics/unified_etl.py \
  ec2-user@44.211.143.180:/data/apple-analytics/
```

### 2. Verify Files on EC2

```bash
ssh -i data_analytics_etl.pem ec2-user@44.211.143.180

# Check the files
grep -A 5 "_should_trust_registry" /data/apple-analytics/src/extract/apple_analytics_client.py
grep "Waiting 10 seconds" /data/apple-analytics/unified_etl.py
```

### 3. Test with Single App

```bash
cd /data/apple-analytics
source /home/ec2-user/anaconda3/bin/activate base
python3 unified_etl.py --app-id 1506886061 --date 2026-01-10
```

### 4. Monitor Next Scheduled Run

```bash
# Cron runs at 09:30 UTC
# Watch the log in real-time
tail -f /data/apple-analytics/logs/cron.log
```

### 5. Verify Results

```bash
# Check latest results JSON
ls -lt /data/apple-analytics/logs/unified_etl_results_*.json | head -1

# View summary
python3 -c "
import json
with open('$(ls -t /data/apple-analytics/logs/unified_etl_results_*.json | head -1)') as f:
    d = json.load(f)
    print(f'Apps Successful: {d[\"apps_successful\"]}/{d[\"apps_processed\"]}')
    print(f'Files Extracted: {d[\"files_extracted\"]}')
"
```

---

## 🎯 SUCCESS CRITERIA

### Must Have (P0):
- [ ] 90%+ apps succeed (target: 92/92 = 100%)
- [ ] Zero cascading 429 errors
- [ ] Registry trust working (logs show "Trusting recent registry")
- [ ] Slack notification shows success

### Nice to Have (P1):
- [ ] Total runtime < 30 minutes
- [ ] All 92 apps succeed on first try (no retries needed)
- [ ] Clear logging of delays and retry attempts

### Monitoring:
- [ ] Check `/data/apple-analytics/logs/cron.log` after tomorrow's 09:30 UTC run
- [ ] Verify Slack notification shows 92/92 success
- [ ] Check no ERROR logs related to 429

---

## 📝 TESTING CHECKLIST

### Local Testing (Before Deploy):
- [x] Code syntax validated (type hints may warn but are correct)
- [x] Import dependencies verified
- [x] Logic flow reviewed

### EC2 Testing (After Deploy):
- [ ] Files uploaded successfully
- [ ] Single app test passes
- [ ] Logs show new features (trust registry, delays, retries)
- [ ] No Python syntax errors

### Production Validation (After Scheduled Run):
- [ ] Cron log shows 92/92 success
- [ ] Slack notification confirms success
- [ ] No 429 errors in logs
- [ ] Data appears in curated S3 paths
- [ ] Athena queries return expected data

---

## 🔄 ROLLBACK PLAN

If issues occur, revert to previous version:

```bash
# On EC2
cd /data/apple-analytics

# Restore from git (if tracked) or from backup
git checkout HEAD~1 src/extract/apple_analytics_client.py unified_etl.py

# Or manually revert specific changes:
# - Remove trust_registry logic
# - Restore old _verify_request_exists()
# - Remove delays
```

---

## 📚 TECHNICAL DETAILS

### Rate Limit Analysis
- **Apple's Limit:** ~20-30 requests/minute (observed)
- **Old Behavior:** 92 apps × 3 calls = 276 potential calls in 5 minutes = **55 calls/min** ❌
- **New Behavior:** 92 apps × 1 call with 10s delays = **6 calls/min** ✅

### Trust Window Rationale
- **30 days:** ONGOING requests don't expire
- **Trade-off:** Longer window = fewer API calls but risk of stale data
- **30 days chosen because:** Apple rarely invalidates ONGOING requests

### Retry Backoff Math
```
Attempt 1: 0s wait
Attempt 2: 5s wait  (2^0 * 5)
Attempt 3: 10s wait (2^1 * 5)
Attempt 4: 20s wait (2^2 * 5)
Max total: 35s per operation
```

---

## ✅ COMPLETION STATUS

- [x] Enhanced registry trust system implemented
- [x] Proper 429 handling added
- [x] Exponential backoff retries added
- [x] 10-second inter-app delays added
- [x] Updated registry metadata loading
- [x] Documentation created
- [ ] Deployed to EC2
- [ ] Tested on EC2
- [ ] Production validated

**Next Step:** Deploy to EC2 and test with single app before scheduled run.
