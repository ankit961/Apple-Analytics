#!/usr/bin/env python3
"""
Apple Analytics Requestor - Production Hardened
- Handles 409 conflicts through S3 registry (no 403 listing dependency)
- Extracts request IDs from existing analytics.json as fallback
- Proper UTC JWT generation
- Normalized S3 paths for curator compatibility
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Optional, Tuple
import requests
import boto3
import jwt
import botocore
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("apple-analytics")

class AppleAnalyticsRequestor:
    """Production-hardened Apple Analytics requestor with S3 registry fallback"""
    
    def __init__(self):
        # Load environment variables from .env file first
        from dotenv import load_dotenv
        load_dotenv()
        
        self.api_base = "https://api.appstoreconnect.apple.com/v1"
        
        # Get AWS configuration from environment
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.s3_bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        
        self.s3_client = boto3.client('s3', region_name=self.aws_region)
        
        # JWT credentials cache
        self.jwt_credentials = None
        self.jwt_expires_at = None
        
        # Load credentials
        self.headers = self._load_credentials()
    
    def _generate_jwt_token(self, issuer_id: str, key_id: str, p8_path: str) -> str:
        """Generate JWT token with proper UTC timezone"""
        try:
            with open(p8_path, 'r') as f:
                private_key = f.read()
            
            # Use timezone-aware UTC datetime
            now = datetime.now(timezone.utc)
            payload = {
                'iss': issuer_id,
                'iat': int(now.timestamp()),
                'exp': int((now + timedelta(minutes=20)).timestamp()),
                'aud': 'appstoreconnect-v1'
            }
            
            headers = {
                'kid': key_id,
                'typ': 'JWT',
                'alg': 'ES256'
            }
            
            token = jwt.encode(payload, private_key, algorithm='ES256', headers=headers)
            return token
            
        except Exception as e:
            logger.error(f"Failed to generate JWT token: {e}")
            raise
        
    def _load_credentials(self) -> Dict[str, str]:
        """Load Apple API credentials and generate JWT token"""
        try:
            with open('.env', 'r') as f:
                env_vars = {}
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
            
            # Get JWT components
            issuer_id = env_vars.get('ASC_ISSUER_ID')
            key_id = env_vars.get('ASC_KEY_ID')
            p8_path = env_vars.get('ASC_P8_PATH')
            
            if not issuer_id or not key_id or not p8_path:
                raise ValueError("Missing JWT credentials: ASC_ISSUER_ID, ASC_KEY_ID, or ASC_P8_PATH")
            
            # Generate JWT token
            token = self._generate_jwt_token(issuer_id, key_id, p8_path)
                
            # Cache JWT components for auto-refresh
            self.issuer_id = issuer_id
            self.key_id = key_id  
            self.p8_path = p8_path
            self.jwt_expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)
            
            return {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            raise
    
    def _need_refresh(self) -> bool:
        """Check if JWT token needs refresh (with 2-minute buffer)"""
        if not self.jwt_expires_at:
            return True
        
        now = datetime.now(timezone.utc)
        buffer_time = timedelta(minutes=2)
        return now >= (self.jwt_expires_at - buffer_time)
    
    def _refresh_headers(self):
        """Regenerate JWT token and update headers"""
        try:
            logger.info("🔄 Refreshing JWT token...")
            token = self._generate_jwt_token(self.issuer_id, self.key_id, self.p8_path)
            self.headers['Authorization'] = f'Bearer {token}'
            self.jwt_expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)
            logger.info("✅ JWT token refreshed")
        except Exception as e:
            logger.error(f"❌ JWT refresh failed: {e}")
            raise
    
    def _asc_request(self, method: str, url: str, max_retries: int = 3, **kwargs):
        """
        Auto-refreshing requests wrapper for Apple API calls
        Handles 401 errors with automatic JWT token renewal
        Includes retry logic for connection errors
        """
        # Check if token needs refresh before making request
        if self._need_refresh():
            self._refresh_headers()
        
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, **kwargs)
                
                # If we get 401, try to refresh token once and retry
                if response.status_code == 401:
                    logger.warning("🔄 Got 401, refreshing token and retrying...")
                    self._refresh_headers()
                    response = requests.request(method, url, headers=self.headers, **kwargs)
                
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
    
    # S3 Registry Helpers
    def _registry_key_for_app(self, app_id: str, access_type: str = "ONE_TIME_SNAPSHOT") -> str:
        """Generate S3 key for request registry"""
        return f"analytics_requests/registry/app_id={app_id}/{access_type.lower()}.json"
    
    def _save_request_registry(self, app_id: str, access_type: str, request_id: str):
        """Save request ID to S3 registry for reuse"""
        key = self._registry_key_for_app(app_id, access_type)
        registry_data = {
            "app_id": app_id,
            "access_type": access_type, 
            "request_id": request_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=json.dumps(registry_data, indent=2),
                ContentType="application/json"
            )
            logger.info("💾 Saved registry: s3://%s/%s", self.s3_bucket, key)
        except Exception as e:
            logger.warning("Failed to save registry: %s", e)
    
    def _load_request_registry(self, app_id: str, access_type: str = "ONE_TIME_SNAPSHOT") -> Optional[Dict]:
        """
        Load request ID and metadata from S3 registry
        
        Returns:
            Dict with 'request_id', 'created_at', 'access_type' or None
        """
        key = self._registry_key_for_app(app_id, access_type)
        
        try:
            obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            rid = data.get("request_id")
            if rid:
                logger.info("📖 Loaded registry for app %s: %s (created: %s)", 
                          app_id, rid, data.get("created_at", "unknown"))
                return data
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchKey":
                logger.warning("Registry load error: %s", e)
        except Exception as e:
            logger.warning("Registry load exception: %s", e)
        
        return None
    
    def _should_trust_registry(self, registry_data: Dict, max_age_days: int = 30) -> bool:
        """
        Determine if we should trust the registry without verification
        
        ONGOING requests don't expire, so we can trust them if:
        - Created within max_age_days (default 30)
        - This avoids unnecessary API calls
        """
        created_at_str = registry_data.get("created_at")
        if not created_at_str:
            return False
        
        try:
            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            age = datetime.now(timezone.utc) - created_at
            
            # Trust if less than max_age_days old
            if age.days < max_age_days:
                logger.info(f"✅ Registry is {age.days} days old, trusting without verification")
                return True
            else:
                logger.info(f"⚠️ Registry is {age.days} days old, will verify")
                return False
        except Exception as e:
            logger.warning(f"⚠️ Error parsing registry age: {e}")
            return False
    
    def _extract_request_id_from_analytics_json(self, app_id: str) -> Optional[str]:
        """Extract request ID from existing analytics.json files as fallback"""
        prefix = f"appstore/raw/analytics/"
        
        try:
            # List dt= folders to find latest analytics data
            resp = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket, 
                Prefix=prefix, 
                Delimiter="/"
            )
            
            # Get dt= prefixes and sort by date (latest first)
            dt_prefixes = [
                c["Prefix"] for c in resp.get("CommonPrefixes", []) 
                if "dt=" in c["Prefix"]
            ]
            
            for dt_prefix in sorted(dt_prefixes, reverse=True):
                analytics_key = f"{dt_prefix}app_id={app_id}/analytics.json"
                
                try:
                    obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=analytics_key)
                    data = json.loads(obj["Body"].read().decode("utf-8"))
                    
                    # Look for ONE_TIME_SNAPSHOT requests in the data
                    for request in data.get("report_requests", []):
                        attrs = request.get("attributes", {})
                        if attrs.get("accessType") == "ONE_TIME_SNAPSHOT":
                            rid = request.get("id")
                            if rid:
                                logger.info("🔎 Found ONE_TIME in %s: %s", analytics_key, rid)
                                return rid
                                
                except ClientError:
                    continue  # Try next dt folder
                    
        except Exception as e:
            logger.warning("extract_request_id_from_analytics_json error: %s", e)
        
        return None
    
    def create_or_reuse_ongoing_request(self, app_id: str) -> Optional[str]:
        """
        Create ONGOING request or reuse existing via S3 registry.
        ONGOING requests are preferred for daily ETL as they don't expire.
        
        OPTIMIZATIONS:
        - Trust registry if created within 30 days (skip verification)
        - Handle 429 rate limits gracefully (don't treat as invalid)
        - Implement exponential backoff for retries
        """
        # 1) Check S3 registry first
        registry_data = self._load_request_registry(app_id, "ONGOING")
        if registry_data:
            existing_rid = registry_data.get("request_id")
            
            # OPTIMIZATION: Trust registry without verification if recent
            if self._should_trust_registry(registry_data, max_age_days=30):
                logger.info("♻️ Trusting recent registry (skip verification): %s", existing_rid)
                return existing_rid
            
            # Verify it's still valid (with proper 429 handling)
            is_valid, reason = self._verify_request_exists(existing_rid, skip_on_rate_limit=True)
            if is_valid:
                logger.info("♻️ Reusing ONGOING request from registry: %s (%s)", existing_rid, reason)
                return existing_rid
            else:
                if reason == 'rate_limited':
                    # Rate limited, but we'll trust the registry anyway
                    logger.warning("⚠️ Verification rate limited, trusting registry: %s", existing_rid)
                    return existing_rid
                else:
                    logger.info("⚠️ Registry ONGOING request %s is invalid (%s), will create new", existing_rid, reason)
        
        # 2) Check Apple API for existing ONGOING request (with retry on 429)
        existing_rid = self._find_existing_ongoing_request_with_retry(app_id)
        if existing_rid:
            # Save to registry for future use
            self._save_request_registry(app_id, "ONGOING", existing_rid)
            return existing_rid
        
        # 3) Create new ONGOING request (with retry on 429)
        return self._create_ongoing_request_with_retry(app_id)
    
    def _find_existing_ongoing_request(self, app_id: str) -> Optional[str]:
        """Find existing ONGOING request via Apple API"""
        try:
            url = f"{self.api_base}/apps/{app_id}/analyticsReportRequests"
            params = {"filter[accessType]": "ONGOING"}
            
            response = self._asc_request('GET', url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                requests_list = data.get('data', [])
                
                if requests_list:
                    rid = requests_list[0]['id']
                    logger.info("🔍 Found existing ONGOING request: %s", rid)
                    return rid
            elif response.status_code == 403:
                logger.warning("⚠️ 403 Forbidden when listing requests - using registry fallback")
            else:
                logger.warning("⚠️ List requests failed: %s", response.status_code)
                
        except Exception as e:
            logger.warning("⚠️ Exception finding ONGOING request: %s", e)
        
        return None
    
    def _find_existing_ongoing_request_with_retry(self, app_id: str, max_retries: int = 3) -> Optional[str]:
        """
        Find existing ONGOING request with exponential backoff on 429
        """
        for attempt in range(max_retries):
            try:
                url = f"{self.api_base}/apps/{app_id}/analyticsReportRequests"
                params = {"filter[accessType]": "ONGOING"}
                
                response = self._asc_request('GET', url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    requests_list = data.get('data', [])
                    
                    if requests_list:
                        rid = requests_list[0]['id']
                        logger.info("🔍 Found existing ONGOING request: %s", rid)
                        return rid
                    return None
                    
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5  # 5, 10, 20 seconds
                        logger.warning(f"⚠️ Rate limited finding ONGOING (attempt {attempt+1}/{max_retries}), waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ Rate limited after {max_retries} attempts")
                        return None
                        
                elif response.status_code == 403:
                    logger.warning("⚠️ 403 Forbidden when listing requests")
                    return None
                else:
                    logger.warning("⚠️ List requests failed: %s", response.status_code)
                    return None
                    
            except Exception as e:
                logger.warning("⚠️ Exception finding ONGOING request: %s", e)
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    time.sleep(wait_time)
                    continue
                return None
        
        return None
    
    def _create_ongoing_request_with_retry(self, app_id: str, max_retries: int = 3) -> Optional[str]:
        """
        Create new ONGOING request with exponential backoff on 429
        """
        payload = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": "ONGOING"},
                "relationships": {"app": {"data": {"type": "apps", "id": str(app_id)}}}
            }
        }
        
        url = f"{self.api_base}/analyticsReportRequests"
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Creating ONGOING request for app {app_id}" + (f" (attempt {attempt+1})" if attempt > 0 else ""))
                
                response = self._asc_request('POST', url, json=payload, timeout=60)
                
                if response.status_code == 201:
                    rid = response.json()["data"]["id"]
                    logger.info("✅ Created ONGOING: %s", rid)
                    
                    # Save to registry for future reuse
                    self._save_request_registry(app_id, "ONGOING", rid)
                    return rid
                    
                elif response.status_code == 409:
                    logger.info("♻️ ONGOING already exists (409). Looking up...")
                    
                    # Try to find it via API
                    existing_rid = self._find_existing_ongoing_request_with_retry(app_id)
                    if existing_rid:
                        self._save_request_registry(app_id, "ONGOING", existing_rid)
                        return existing_rid
                        
                    logger.error("409 conflict but could not find existing ONGOING request")
                    return None
                    
                elif response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5  # 5, 10, 20 seconds
                        logger.warning(f"⚠️ Rate limited creating ONGOING (attempt {attempt+1}/{max_retries}), waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"❌ Create ONGOING rate limited after {max_retries} attempts")
                        return None
                        
                else:
                    logger.error("❌ Create ONGOING failed %s: %s", response.status_code, response.text[:600])
                    return None
                    
            except Exception as e:
                logger.error("❌ Exception creating ONGOING request: %s", e)
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    time.sleep(wait_time)
                    continue
                return None
        
        return None
    
    def _verify_request_exists(self, request_id: str, skip_on_rate_limit: bool = True) -> Tuple[bool, str]:
        """
        Verify a request ID is still valid
        
        Returns:
            tuple[bool, str]: (is_valid, reason)
            - (True, 'valid') - Request exists and is valid
            - (False, 'rate_limited') - Hit 429, can't verify but might be valid
            - (False, 'not_found') - Request doesn't exist (404)
            - (False, 'error') - Other error
        """
        try:
            url = f"{self.api_base}/analyticsReportRequests/{request_id}"
            response = self._asc_request('GET', url, timeout=30)
            
            if response.status_code == 200:
                return (True, 'valid')
            elif response.status_code == 429:
                # Rate limited - we can't verify, but don't assume invalid
                if skip_on_rate_limit:
                    logger.warning("⚠️ Rate limited during verification, assuming valid")
                    return (True, 'rate_limited')  # Assume valid to avoid cascade
                return (False, 'rate_limited')
            elif response.status_code == 404:
                return (False, 'not_found')
            else:
                return (False, f'status_{response.status_code}')
        except Exception as e:
            logger.warning(f"⚠️ Exception during verification: {e}")
            return (False, 'error')

    def create_or_reuse_one_time_request(self, app_id: str, start_date: str, end_date: str) -> Optional[str]:
        """
        Create ONE_TIME_SNAPSHOT or reuse existing via S3 registry (no 403 listing dependency)
        """
        # 1) Try to create minimal request first
        payload = {
            "data": {
                "type": "analyticsReportRequests",
                "attributes": {"accessType": "ONE_TIME_SNAPSHOT"},
                "relationships": {"app": {"data": {"type": "apps", "id": str(app_id)}}}
            }
        }
        
        url = f"{self.api_base}/analyticsReportRequests"
        logger.info("Creating ONE_TIME request for app %s: %s → %s", app_id, start_date, end_date)
        
        try:
            response = self._asc_request('POST', url, json=payload, timeout=60)
            
            if response.status_code == 201:
                rid = response.json()["data"]["id"]
                logger.info("✅ Created ONE_TIME: %s", rid)
                
                # Save to registry for future reuse
                self._save_request_registry(app_id, "ONE_TIME_SNAPSHOT", rid)
                self._save_request_state(rid, app_id, start_date, end_date, "CREATED")
                return rid
                
            elif response.status_code == 409:
                logger.info("♻️ ONE_TIME already exists. Reusing from registry or known files.")
                
                # 2) Try S3 registry first
                rid = self._load_request_registry(app_id, "ONE_TIME_SNAPSHOT") 
                if rid:
                    return rid
                
                # 3) Fallback: extract from existing analytics.json
                rid = self._extract_request_id_from_analytics_json(app_id)
                if rid:
                    # Save to registry for next time
                    self._save_request_registry(app_id, "ONE_TIME_SNAPSHOT", rid)
                    return rid
                    
                logger.error("409 conflict but no way to discover existing request_id")
                return None
                
            else:
                logger.error("❌ Create ONE_TIME failed %s: %s", response.status_code, response.text[:600])
                return None
                
        except Exception as e:
            logger.error("❌ Exception creating request: %s", e)
            return None
    
    def fetch_sales_and_trends_data(self, app_id: str, start_date: str, end_date: str) -> Dict[str, int]:
        """
        Fetch Sales & Trends data with proper 404 handling
        """
        # Load vendor number from environment
        try:
            with open('.env', 'r') as f:
                env_vars = {}
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        env_vars[key] = value.strip('"\'')
            
            vendor_number = env_vars.get('ASC_VENDOR_NUMBER') or env_vars.get('APPLE_VENDOR_NUMBER')
            if not vendor_number:
                logger.error("❌ Missing ASC_VENDOR_NUMBER/APPLE_VENDOR_NUMBER")
                return {}
                
        except Exception as e:
            logger.error(f"❌ Failed to load vendor number: {e}")
            return {}
        
        files_summary = {'sales_and_trends_files': 0}
        
        # Parse date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Sales & Trends API call
            url = f"{self.api_base}/salesReports"
            params = {
                'filter[frequency]': 'DAILY',
                'filter[reportDate]': date_str,
                'filter[reportSubType]': 'SUMMARY',
                'filter[reportType]': 'SALES',
                'filter[vendorNumber]': vendor_number
            }
            
            try:
                response = self._asc_request('GET', url, params=params, timeout=60)
                
                if response.status_code == 200:
                    # Sales data comes as compressed TSV
                    content = response.content
                    
                    if len(content) > 0:
                        # Normalized S3 path for curator compatibility
                        s3_key = f"appstore/raw_sales/SALES/DAILY/reportDate={date_str}/sales_{date_str}.tsv.gz"
                        
                        self.s3_client.put_object(
                            Bucket=self.s3_bucket,
                            Key=s3_key,
                            Body=content,
                            ContentType='application/gzip'
                        )
                        
                        logger.info("✅ Sales data for %s: s3://%s/%s (bytes=%d)", 
                                  date_str, self.s3_bucket, s3_key, len(content))
                        files_summary['sales_and_trends_files'] += 1
                    else:
                        logger.info("⚠️ Empty sales response for %s", date_str)
                        
                elif response.status_code == 404:
                    # 404 is normal for no-sales days or not-yet-available dates
                    logger.info("ℹ️ Sales DAILY not available for %s (no data or not published yet)", date_str)
                else:
                    logger.error("Sales fetch failed %s: %s %s", 
                               date_str, response.status_code, response.text[:300])
                    
            except Exception as e:
                logger.error("❌ Exception fetching sales for %s: %s", date_str, e)
            
            current_date += timedelta(days=1)
        
        logger.info("📊 Sales & Trends summary: %s", files_summary)
        return files_summary
    
    def poll_request_completion(self, request_id: str, max_polls: int = 60) -> bool:
        """
        Schema-tolerant polling that handles missing 'status' field
        Falls back to instance availability check when status is unavailable
        """
        url = f"{self.api_base}/analyticsReportRequests/{request_id}"
        
        for poll_count in range(max_polls):
            try:
                response = self._asc_request('GET', url, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    attrs = data['data']['attributes']
                    
                    # Schema-tolerant status extraction
                    status = attrs.get('status') or attrs.get('state')
                    
                    if status:
                        logger.info("Poll %d/%d: %s", poll_count + 1, max_polls, status)
                        
                        if status == 'COMPLETED':
                            logger.info("✅ Request %s completed", request_id)
                            self._update_request_state(request_id, "COMPLETED")
                            return True
                            
                        elif status == 'FAILED':
                            logger.error("❌ Request %s failed", request_id)
                            self._update_request_state(request_id, "FAILED")
                            return False
                            
                        elif status in ['PROCESSING', 'SCHEDULED']:
                            # Still processing
                            time.sleep(30)
                            continue
                    else:
                        # Fallback: Check if instances are available (indicates completion)
                        logger.info("Poll %d/%d: No status field, checking instances...", 
                                  poll_count + 1, max_polls)
                        
                        if self._check_instances_availability(request_id):
                            logger.info("✅ Request %s completed (via instance check)", request_id)
                            self._update_request_state(request_id, "COMPLETED")
                            return True
                        else:
                            logger.info("⏳ Instances not ready, continuing...")
                            time.sleep(30)
                            continue
                        
                else:
                    logger.error("❌ Poll failed: %s - %s", response.status_code, response.text[:300])
                    
            except Exception as e:
                logger.error("❌ Poll exception: %s", e)
                
            time.sleep(30)
        
        logger.warning("⏱️ Request %s timeout after %d polls", request_id, max_polls)
        return False
    
    def _check_instances_availability(self, request_id: str) -> bool:
        """
        Fallback method to check if request is complete by testing instance availability
        """
        instances_url = f"{self.api_base}/analyticsReportRequests/{request_id}/instances"
        
        try:
            response = self._asc_request('GET', instances_url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                instances = data.get('data', [])
                
                if len(instances) > 0:
                    logger.info("🎯 Found %d instances - request is complete", len(instances))
                    return True
                else:
                    logger.info("⏳ No instances yet - request still processing")
                    return False
                    
            elif response.status_code == 404:
                # 404 means request isn't ready yet
                return False
            else:
                logger.warning("Instance check failed: %s", response.status_code)
                return False
                
        except Exception as e:
            logger.warning("Instance availability check failed: %s", e)
            return False
    
    def download_analytics_files(self, request_id: str, app_id: str) -> int:
        """
        Download analytics files using instances → segments → files traversal
        Uses normalized S3 paths for curator compatibility
        """
        total_files = 0
        
        # Get instances for the request
        instances_url = f"{self.api_base}/analyticsReportRequests/{request_id}/instances"
        
        try:
            response = self._asc_request('GET', instances_url, timeout=60)
            response.raise_for_status()
            
            instances_data = response.json()
            instances = instances_data.get('data', [])
            
            logger.info("Processing %d instances for request %s", len(instances), request_id)
            
            for instance in instances:
                instance_id = instance['id']
                
                # Get segments for this instance
                segments_url = f"{self.api_base}/analyticsReportInstances/{instance_id}/segments"
                seg_response = self._asc_request('GET', segments_url, timeout=30)
                
                if seg_response.status_code == 200:
                    segments_data = seg_response.json()
                    segments = segments_data.get('data', [])
                    
                    for segment in segments:
                        segment_id = segment['id']
                        
                        # Get files for this segment
                        files_url = f"{self.api_base}/analyticsReportInstances/{instance_id}/segments/{segment_id}/files"
                        files_response = self._asc_request('GET', files_url, timeout=30)
                        
                        if files_response.status_code == 200:
                            files_data = files_response.json()
                            files = files_data.get('data', [])
                            
                            for file_obj in files:
                                attrs = file_obj.get('attributes', {})
                                download_url = attrs.get('downloadUrl') or attrs.get('url')
                                
                                if download_url:
                                    # Normalized S3 path for analytics
                                    s3_key = f"appstore/raw/analytics/engagement/request_id={request_id}/app_id={app_id}/instance_id={instance_id}/segment_id={segment_id}.csv"
                                    
                                    if self._download_and_upload_to_s3(download_url, s3_key):
                                        total_files += 1
            
            logger.info("✅ Downloaded %d analytics files", total_files)
            return total_files
            
        except Exception as e:
            logger.error("❌ Exception downloading analytics files: %s", e)
            return 0
    
    def _download_and_upload_to_s3(self, download_url: str, s3_key: str) -> bool:
        """Download file from signed URL and upload to S3"""
        try:
            # Download from signed URL (no auth needed)
            response = requests.get(download_url, timeout=120)
            
            if response.status_code != 200:
                logger.error("❌ Failed to download from %s: %s", download_url, response.status_code)
                return False
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=response.content,
                ContentType='text/csv'
            )
            
            logger.info("⬆️ Raw landed: s3://%s/%s", self.s3_bucket, s3_key)
            return True
            
        except Exception as e:
            logger.error("❌ Exception uploading to S3: %s", e)
            return False
    
    def _save_request_state(self, request_id: str, app_id: str, start_date: str, 
                           end_date: str, status: str):
        """Save request state tracking to S3"""
        state = {
            'request_id': request_id,
            'app_id': app_id,
            'start_date': start_date,
            'end_date': end_date,
            'status': status,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        s3_key = f"analytics_requests/state/{request_id}.json"
        
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(state, indent=2),
                ContentType='application/json'
            )
        except Exception as e:
            logger.warning("Failed to save request state: %s", e)
    
    def _update_request_state(self, request_id: str, status: str):
        """Update request state in S3"""
        s3_key = f"analytics_requests/state/{request_id}.json"
        
        try:
            # Get existing state
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            state = json.loads(response['Body'].read().decode('utf-8'))
            
            # Update status
            state['status'] = status
            state['updated_at'] = datetime.now(timezone.utc).isoformat()
            
            # Save back to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(state, indent=2),
                ContentType='application/json'
            )
            
        except Exception as e:
            logger.warning("Failed to update request state: %s", e)

def main():
    """Production execution with registry-based request reuse"""
    print("🍎 APPLE ANALYTICS REQUESTOR - PRODUCTION HARDENED")
    print("=" * 70)
    print("🔧 Registry-based request reuse (no 403 listing dependency)")
    print("📊 Normalized S3 paths for curator compatibility") 
    print("⏰ Proper UTC JWT generation")
    print("=" * 70)
    
    requestor = AppleAnalyticsRequestor()
    
    # Configuration
    app_id = "1506886061"  # Doctor Games for Kids
    
    # Use historical dates that we know have data (avoid recent 404s)
    start_date = date(2025, 10, 20)  # Historical date with confirmed data
    end_date = date(2025, 10, 25)    # 5-day test window
    
    print(f"\n📱 App ID: {app_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Using historical dates to avoid recent 404s\n")
    
    success_summary = {
        'analytics_request': False,
        'analytics_files': 0,
        'sales_files': 0,
        'total_files': 0
    }
    
    # PART 1: Analytics API with registry-based reuse
    print("🔍 PART 1: Analytics (Discovery & Engagement)")
    print("-" * 50)
    
    request_id = requestor.create_or_reuse_one_time_request(
        app_id=app_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat()
    )
    
    if request_id:
        print(f"✅ Analytics request ready: {request_id}")
        success_summary['analytics_request'] = True
        
        # Poll for completion
        print("🔄 Polling for completion...")
        completed = requestor.poll_request_completion(request_id, max_polls=60)
        
        if completed:
            # Download files
            print("📥 Downloading analytics files...")
            analytics_files = requestor.download_analytics_files(request_id, app_id)
            success_summary['analytics_files'] = analytics_files
            print(f"✅ Analytics files: {analytics_files}")
        else:
            print("⚠️ Analytics request timed out, continuing...")
    else:
        print("❌ Analytics request not available")
    
    # PART 2: Sales & Trends API
    print(f"\n📊 PART 2: Sales & Trends (Downloads)")
    print("-" * 50)
    
    sales_summary = requestor.fetch_sales_and_trends_data(
        app_id=app_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat()
    )
    success_summary['sales_files'] = sales_summary.get('sales_and_trends_files', 0)
    success_summary['total_files'] = success_summary['analytics_files'] + success_summary['sales_files']
    
    # FINAL RESULTS
    print(f"\n" + "=" * 70)
    print("📊 FINAL RESULTS")
    print("=" * 70)
    print(f"✅ Analytics files: {success_summary['analytics_files']}")
    print(f"📱 Sales files: {success_summary['sales_files']}")
    print(f"📂 Total files: {success_summary['total_files']}")
    
    if success_summary['total_files'] > 0:
        print(f"\n🎯 NEXT STEPS:")
        print(f"1. Run curator: python3 apple_analytics_data_curator_production.py")
        print(f"2. Build unified table: python3 athena_table_manager_production.py") 
        print(f"3. Verify dashboard: http://localhost:3000")
        print(f"\n📁 S3 Locations:")
        print(f"   Analytics: s3://skidos-apptrack/appstore/raw/analytics/")
        print(f"   Sales: s3://skidos-apptrack/appstore/raw_sales/")
    else:
        print(f"\n⚠️ No new files downloaded")
        print(f"💡 Existing data may be sufficient for dashboard operation")

if __name__ == "__main__":
    main()
