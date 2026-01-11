# Apple Analytics ETL - Scheduler & Transform Audit
**Date:** 2026-01-11  
**Environment:** EC2 Production (`44.211.143.180`)

---

## ✅ SCHEDULER STATUS

### Crontab Configuration
```bash
30 9 * * * /data/apple-analytics/daily_cron.sh >> /data/apple-analytics/logs/cron.log 2>&1
```

**Schedule:** Daily at 09:30 UTC (3:00 PM IST)

### Script Location
- **Path:** `/data/apple-analytics/daily_cron.sh`
- **ETL Script:** `/data/apple-analytics/unified_etl.py`
- **Client:** `/data/apple-analytics/src/extract/apple_analytics_client.py`

### Features
✅ Conda environment activation (`source /home/ec2-user/anaconda3/bin/activate base`)  
✅ Slack notifications (start/success/failure)  
✅ Results parsing from JSON  
✅ Duration tracking  
✅ Proper error handling and exit codes

---

## ✅ TRANSFORM LOGIC VERIFICATION

### File Filtering (STANDARD vs DETAILED)

**Location:** `unified_etl.py` → `_curate_app_data_from_processing_date()`

```python
for obj in response.get('Contents', []):
    if not obj['Key'].endswith('.csv'):
        continue
    
    filename = obj['Key'].split('/')[-1].lower()
    
    # Only use STANDARD files (complete data)
    # Skip DETAILED files (only attributed data, ~15% of total)
    if 'detailed' in filename or 'performance' in filename:
        continue
```

**✅ CORRECT:** The logic properly filters for STANDARD files only.

### What Gets Skipped:
- ❌ Files with `detailed` in name (attribution-only subset)
- ❌ Files with `performance` in name (different metrics)

### What Gets Processed:
- ✅ `app_downloads_standard_*.csv`
- ✅ `app_store_installation_and_deletion_standard_*.csv`
- ✅ `app_store_discovery_and_engagement_standard_*.csv`
- ✅ `app_sessions_standard_*.csv`
- ✅ `app_store_purchases_standard_*.csv`

---

## ❌ CRITICAL ISSUE: Rate Limiting Cascade

### The Problem

**Current Flow (per app):**
```python
def create_or_reuse_ongoing_request(app_id: str):
    # 1) Load registry
    existing_rid = self._load_request_registry(app_id, "ONGOING")
    
    if existing_rid:
        # 2) VERIFY (makes GET request to Apple)
        if self._verify_request_exists(existing_rid):  # ← API CALL #1
            return existing_rid
        else:
            logger.info("⚠️ Registry ONGOING request %s is invalid")
    
    # 3) Find existing (makes GET request to Apple)
    existing_rid = self._find_existing_ongoing_request(app_id)  # ← API CALL #2
    if existing_rid:
        return existing_rid
    
    # 4) Create new (makes POST request to Apple)
    response = self._asc_request('POST', url, ...)  # ← API CALL #3
```

### The _verify_request_exists() Problem

```python
def _verify_request_exists(self, request_id: str) -> bool:
    try:
        url = f"{self.api_base}/analyticsReportRequests/{request_id}"
        response = self._asc_request('GET', url, timeout=30)
        return response.status_code == 200
    except Exception:
        return False  # ⚠️ TREATS 429 AS "INVALID"!
```

**Issue:** When Apple returns `429 RATE_LIMIT_EXCEEDED`, the exception is caught and `False` is returned, making the code think the request is invalid.

### Verification Results

We tested 5 apps that showed "invalid" errors:
- **App 6449359840:** ✅ VALID (request created 2025-11-29)
- **App 6448038513:** ✅ VALID (request created 2026-01-11)
- **App 6469684995:** ✅ VALID (request created 2025-11-29)
- **App 6466577779:** ✅ VALID (request created 2025-11-29)
- **App 1506886061:** ✅ VALID (request created 2025-11-27)

**Conclusion:** ALL requests in registry are valid. The "invalid" errors are false positives caused by rate limiting during verification.

---

## 📊 RECENT RUN RESULTS

### Jan 11, 2026 (09:30 UTC)
```
Apps Processed:  92
Apps Successful: 27  (29%)
Apps Failed:     65  (71%)
Files Extracted: 5,760
Files Curated:   106
Total Rows:      307,998
```

### Failure Pattern
- **First ~20-30 apps:** Succeed (before hitting rate limit)
- **Remaining ~65 apps:** Fail with cascading 429 errors:
  1. `_verify_request_exists()` → 429 → returns `False`
  2. `_find_existing_ongoing_request()` → 429
  3. `POST create new` → 429
  4. App extraction fails ❌

---

## 🎯 ROOT CAUSE SUMMARY

| Issue | Impact | Evidence |
|-------|--------|----------|
| **No delays between apps** | 92 sequential API calls in rapid succession | Logs show apps processed back-to-back |
| **Aggressive verification** | Every app verified on every run | `_verify_request_exists()` called 92× |
| **Poor 429 handling** | Rate limits treated as "invalid requests" | Exception catches 429, returns `False` |
| **Cascading failures** | 3 API calls per "failed" verification | verify → find → create (all hit 429) |

**Total API Calls per Run:** Up to **276 calls** (92 apps × 3 potential calls)  
**Apple Rate Limit Threshold:** ~20-30 requests before throttling kicks in

---

## ✅ WHAT'S WORKING

1. ✅ **Transform logic correctly filters STANDARD files only**
2. ✅ **Cron scheduler properly configured and running**
3. ✅ **Slack notifications working**
4. ✅ **Registry system storing valid request IDs**
5. ✅ **All ONGOING requests are actually valid (don't expire)**
6. ✅ **27 apps succeed (those processed before rate limit)**

---

## 🔧 REQUIRED FIXES

### Priority 1: Fix Rate Limiting Issues

1. **Trust the Registry** - Skip verification for recently created requests
2. **Handle 429 Properly** - Don't treat rate limits as "invalid"
3. **Add Delays** - Space out API calls between apps
4. **Exponential Backoff** - Retry 429s with increasing delays

### Priority 2: Improve Error Reporting

1. **Capture per-app failures** in results JSON
2. **Include error reasons** (429 vs actual failures)
3. **Better Slack notifications** with failure details

---

## 📋 NEXT STEPS

1. ✅ **Audit Complete** - Scheduler and transform logic verified
2. ⏭️ **Implement Fixes** - Update `apple_analytics_client.py` with:
   - Registry trust (skip verification if created <7 days ago)
   - Proper 429 handling
   - Inter-app delays
   - Exponential backoff for retries
3. ⏭️ **Deploy to EC2** - Update production code
4. ⏭️ **Test** - Monitor next scheduled run
5. ⏭️ **Verify** - Check 92/92 apps succeed

---

## 🎯 EXPECTED OUTCOME AFTER FIXES

**Before:**
- 27/92 apps succeed (29%)
- 65 apps fail with 429 cascading errors

**After Fixes:**
- 92/92 apps succeed (100%)
- No rate limit errors
- Faster execution (no unnecessary API calls)
