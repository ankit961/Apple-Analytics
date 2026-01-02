#!/usr/bin/env python3
"""
Apple Analytics - Unified Production ETL Pipeline
==================================================

This is the SINGLE script for complete ETL supporting BOTH request types:
1. EXTRACT: Fetch data from Apple Analytics API (ONGOING or ONE_TIME_SNAPSHOT)
2. TRANSFORM: Convert raw CSV/JSON to optimized Parquet
3. LOAD: Refresh Athena table partitions

ONGOING Requests (Daily Automation):
- Day 1: Creates new ONGOING request, saves to S3 registry
- Day 2+: Reuses same request from registry (ONGOING requests don't expire)
- Apple adds new report instances daily, we fetch by processingDate
- Use when: Running daily automated pipelines

ONE_TIME_SNAPSHOT Requests (Backfill/Bulk Export):
- Creates request with explicit start_date and end_date
- Apple processes entire date range and provides all data at once
- Reports available with instances for each date in range
- Use when: Backfilling historical data, one-time exports, bulk recovery

Usage:
    # ONGOING Mode (Default)
    python3 unified_etl.py                                  # Yesterday's data
    python3 unified_etl.py --date 2025-11-27                # Specific date
    python3 unified_etl.py --app-id 1506886061              # Specific app
    python3 unified_etl.py --backfill --days 30             # Last 30 days (ONGOING)
    
    # ONE_TIME_SNAPSHOT Mode (Backfill/Bulk)
    python3 unified_etl.py --onetime --start-date 2025-11-01 --end-date 2025-11-30
    python3 unified_etl.py --onetime --start-date 2025-11-01 --end-date 2025-11-30 --app-id 1506886061
"""

import os
import sys
import io
import json
import gzip
import argparse
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import boto3
import pandas as pd
import requests as http_requests
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))
load_dotenv(os.path.join(os.path.dirname(SCRIPT_DIR), '.env'))

# Ensure logs directory exists
os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'logs', f'unified_etl_{datetime.now().strftime("%Y%m%d")}.log'))
    ]
)
logger = logging.getLogger(__name__)

# Import Apple Analytics client
from src.extract.apple_analytics_client import AppleAnalyticsRequestor


class UnifiedETL:
    """
    Unified ETL Pipeline for Apple Analytics
    
    Supports BOTH request types:
    1. ONGOING: For daily automated pipelines (creates once, reuses forever)
    2. ONE_TIME_SNAPSHOT: For backfills and bulk exports (explicit date range)
    
    Complete flow:
    1. EXTRACT: Creates/reuses appropriate request type, downloads data
    2. TRANSFORM: Converts raw CSV to Parquet with deduplication
    3. LOAD: Refreshes Athena partitions
    
    Use ONGOING (default) for daily automation.
    Use ONE_TIME_SNAPSHOT for backfilling historical data or one-time exports.
    """
    
    def __init__(self):
        self.requestor = AppleAnalyticsRequestor()
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.athena = boto3.client('athena', region_name='us-east-1')
        self.bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        self.athena_output = os.getenv('ATHENA_OUTPUT', 's3://skidos-apptrack/Athena-Output/')
        
        # Results tracking
        self.results = {
            'start_time': datetime.now(timezone.utc).isoformat(),
            'apps_processed': 0,
            'apps_successful': 0,
            'files_extracted': 0,
            'files_curated': 0,
            'total_rows': 0,
            'errors': [],
            'request_ids': {}
        }
    
    def get_app_ids(self, specific_app_id: Optional[str] = None) -> List[str]:
        """Get list of app IDs to process"""
        if specific_app_id:
            return [specific_app_id]
        
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        
        return ['1506886061']
    
    def generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end (inclusive)"""
        dates = []
        current = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
    
    def _validate_request_is_available(self, request_id: str) -> bool:
        """Check if a request ID is still valid and accessible"""
        try:
            # Use correct endpoint: analyticsReportRequests (not analyticsRequests)
            status_url = f"{self.requestor.api_base}/analyticsReportRequests/{request_id}"
            resp = self.requestor._asc_request('GET', status_url, timeout=30)
            
            if resp is None:
                logger.warning(f"   ⚠️ No response validating request {request_id}")
                return False
            
            if resp.status_code == 200:
                return True
            elif resp.status_code == 404:
                logger.warning(f"   ⚠️ Request {request_id} no longer available (404)")
                return False
            else:
                logger.warning(f"   ⚠️ Request validation returned {resp.status_code}")
                return False
        except Exception as e:
            logger.warning(f"   ⚠️ Request validation error: {e}")
            return False
    
    def create_onetime_request_for_range(self, app_id: str, start_date: str, end_date: str) -> Optional[str]:
        """
        Create or reuse ONE_TIME_SNAPSHOT request for date range
        Saves request ID to registry for future reference
        
        Process:
        1. Try to load request from registry
        2. Validate it's still accessible
        3. If not valid or doesn't exist, create new request
        """
        try:
            logger.info(f"📱 Creating ONE_TIME_SNAPSHOT for app {app_id}: {start_date} → {end_date}")
            
            # Step 1: Try to load from registry
            reg_key = f"analytics_requests/registry/app_id={app_id}/one_time_snapshot.json"
            
            existing_request_id = None
            try:
                resp = self.s3.get_object(Bucket=self.bucket, Key=reg_key)
                registry = json.load(resp['Body'])
                existing_request_id = registry.get('request_id')
                logger.info(f"   ♻️ Found existing request in registry: {existing_request_id}")
            except:
                pass
            
            # Step 2: Validate existing request or create new one
            if existing_request_id:
                if self._validate_request_is_available(existing_request_id):
                    logger.info(f"   ✅ Using existing request: {existing_request_id}")
                    request_id = existing_request_id
                else:
                    logger.info(f"   🔄 Existing request no longer valid, creating new one")
                    request_id = self.requestor.create_or_reuse_one_time_request(app_id, start_date, end_date)
            else:
                # Step 3: Create new request
                request_id = self.requestor.create_or_reuse_one_time_request(app_id, start_date, end_date)
            
            if request_id:
                logger.info(f"   ✅ Request ID: {request_id}")
                self.results['request_ids'][app_id] = {
                    'request_id': request_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'created_at': datetime.now(timezone.utc).isoformat()
                }
                return request_id
            else:
                logger.error(f"   ❌ Failed to create ONE_TIME_SNAPSHOT request")
                self.results['errors'].append(f"Failed to create request for {app_id}")
                return None
                
        except Exception as e:
            logger.error(f"   ❌ Exception creating request: {e}")
            self.results['errors'].append(str(e))
            import traceback
            traceback.print_exc()
            return None
    
    # =========================================================================
    # EXTRACT PHASE - Using proven logic from daily_etl.py
    # =========================================================================
    
    def extract_app_data(self, app_id: str, target_date: str) -> Dict:
        """Extract all data for a single app and date (with retry logic)"""
        result = {
            'app_id': app_id,
            'date': target_date,
            'files': 0,
            'rows': 0,
            'success': False,
            'errors': []
        }
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                logger.info(f"📱 Extracting app {app_id} for {target_date}" + (f" (attempt {attempt+1})" if attempt > 0 else ""))
                
                # Get or create ONGOING request (reuses from registry if exists)
                request_id = self.requestor.create_or_reuse_ongoing_request(app_id)
                if not request_id:
                    result['errors'].append(f"Failed to get request for {app_id}")
                    return result
                
                logger.info(f"   Using request: {request_id}")
                
                # Get all reports for this request
                reports_url = f"{self.requestor.api_base}/analyticsReportRequests/{request_id}/reports"
                response = self.requestor._asc_request('GET', reports_url, timeout=60)
                
                if response.status_code != 200:
                    result['errors'].append(f"Failed to get reports: {response.status_code}")
                    return result
                
                reports = response.json().get('data', [])
                logger.info(f"   Found {len(reports)} reports")
                
                # Process each report
                for report in reports:
                    report_id = report['id']
                    report_name = report['attributes']['name']
                    
                    # Get instances for this report with DAILY granularity filter
                    instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                    inst_response = self.requestor._asc_request('GET', instances_url, 
                                                                params={'filter[granularity]': 'DAILY'},
                                                                timeout=30)
                    
                    if inst_response.status_code != 200:
                        continue
                    
                    instances = inst_response.json().get('data', [])
                    
                    # CRITICAL: Filter instances by target_date to avoid writing wrong data
                    matching_instances = []
                    for inst in instances:
                        processing_date = (inst.get("attributes", {}) or {}).get("processingDate")
                        if processing_date and processing_date.split("T")[0] == target_date:
                            matching_instances.append(inst)
                    
                    # Download data only from matching instances
                    for instance in matching_instances:
                        instance_id = instance['id']
                        files, rows = self._download_instance_data(
                            instance_id, app_id, report_name, target_date
                        )
                        if files > 0:
                            result['files'] += files
                            result['rows'] += rows
                
                # Success - exit retry loop
                if result['files'] > 0:
                    result['success'] = True
                    logger.info(f"   ✅ Extracted {result['files']} files, {result['rows']:,} rows")
                else:
                    logger.info(f"   ⚠️ No new data for {target_date}")
                    result['success'] = True  # Not an error, just no new data
                
                return result
                
            except (ConnectionError, TimeoutError) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"   ⚠️ Connection error (attempt {attempt+1}/{max_retries}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    result['errors'].append(f"Connection failed after {max_retries} attempts: {str(e)}")
                    logger.error(f"   ❌ Connection error: {e}")
                    
            except Exception as e:
                result['errors'].append(str(e))
                logger.error(f"   ❌ Error: {e}")
                return result
        
        return result
    
    def _download_instance_data(self, instance_id: str, app_id: str, 
                                report_name: str, target_date: str) -> Tuple[int, int]:
        """Download data from an instance - proven logic from daily_etl.py"""
        files_downloaded = 0
        total_rows = 0
        
        try:
            # Get segments for this instance
            segments_url = f"{self.requestor.api_base}/analyticsReportInstances/{instance_id}/segments"
            seg_response = self.requestor._asc_request('GET', segments_url, timeout=30)
            
            if seg_response.status_code != 200:
                return 0, 0
            
            segments = seg_response.json().get('data', [])
            
            for segment in segments:
                segment_id = segment['id']
                seg_attrs = segment.get('attributes', {})
                
                # Get download URL
                download_url = seg_attrs.get('url') or seg_attrs.get('downloadUrl')
                
                if not download_url:
                    # Try fetching segment details
                    seg_detail_url = f"{self.requestor.api_base}/analyticsReportSegments/{segment_id}"
                    seg_detail = self.requestor._asc_request('GET', seg_detail_url, timeout=30)
                    if seg_detail.status_code == 200:
                        seg_detail_attrs = seg_detail.json()['data']['attributes']
                        download_url = seg_detail_attrs.get('url') or seg_detail_attrs.get('downloadUrl')
                
                if download_url:
                    rows = self._download_and_save(download_url, app_id, report_name, 
                                                   instance_id, segment_id, target_date)
                    if rows > 0:
                        files_downloaded += 1
                        total_rows += rows
                        
        except Exception as e:
            logger.debug(f"Instance {instance_id} error: {e}")
        
        return files_downloaded, total_rows
    
    def _download_and_save(self, download_url: str, app_id: str, report_name: str,
                          instance_id: str, segment_id: str, target_date: str) -> int:
        """Download file from URL and save to S3"""
        try:
            response = http_requests.get(download_url, timeout=120)
            if response.status_code != 200:
                return 0
            
            content = response.content
            
            # Handle gzip compression
            if content[:2] == b'\x1f\x8b':
                try:
                    content = gzip.decompress(content)
                except Exception:
                    pass
            
            # Decode text
            try:
                text_content = content.decode('utf-8')
            except UnicodeDecodeError:
                text_content = content.decode('latin-1', errors='replace')
            
            lines = text_content.strip().split('\n')
            if len(lines) <= 1:
                return 0
            
            row_count = len(lines) - 1
            
            # Determine report type for S3 path
            report_type = self._get_report_type(report_name)
            
            # Clean report name for file path
            clean_name = "".join(c for c in report_name if c.isalnum() or c in ' -_').replace(' ', '_').lower()
            
            # S3 key
            s3_key = f"appstore/raw/{report_type}/dt={target_date}/app_id={app_id}/{clean_name}_{segment_id}.csv"
            
            # Upload to S3
            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=text_content.encode('utf-8'),
                ContentType='text/csv'
            )
            
            logger.info(f"      ✅ {report_type}/{clean_name}: {row_count} rows")
            return row_count
            
        except Exception as e:
            logger.debug(f"Download error: {e}")
            return 0
    
    def _get_report_type(self, report_name: str) -> str:
        """Map report name to data type"""
        report_lower = report_name.lower()
        if 'download' in report_lower:
            return 'downloads'
        elif 'engagement' in report_lower or 'discovery' in report_lower or 'impression' in report_lower:
            return 'engagement'
        elif 'session' in report_lower:
            return 'sessions'
        elif 'install' in report_lower:
            return 'installs'
        elif 'purchase' in report_lower or 'subscription' in report_lower:
            return 'purchases'
        elif 'review' in report_lower or 'rating' in report_lower:
            return 'reviews'
        else:
            return 'analytics'
    
    # =========================================================================
    # EXTRACT PHASE - ONE_TIME_SNAPSHOT extraction
    # =========================================================================
    
    def _extract_onetime_data(self, app_id: str, request_id: str, target_date: str) -> Dict:
        """
        Extract data from ONE_TIME_SNAPSHOT request for specific date
        
        Args:
            app_id: Application ID
            request_id: ONE_TIME_SNAPSHOT request ID
            target_date: Target date to extract (YYYY-MM-DD)
        
        Returns:
            Dictionary with extraction results
        """
        result = {
            'app_id': app_id,
            'request_id': request_id,
            'date': target_date,
            'files': 0,
            'rows': 0,
            'success': False,
            'errors': []
        }
        
        try:
            # Get all reports for this request
            reports_url = f"{self.requestor.api_base}/analyticsReportRequests/{request_id}/reports"
            response = self.requestor._asc_request('GET', reports_url, timeout=60)
            
            if response is None or response.status_code != 200:
                result['errors'].append(f"Failed to get reports: {response.status_code if response else 'No response'}")
                logger.warning(f"   ⚠️ Failed to get reports for {request_id}")
                return result
            
            reports = response.json().get('data', [])
            logger.info(f"   📊 Found {len(reports)} reports for {target_date}")
            
            # Process each report
            for report in reports:
                report_id = report['id']
                report_name = report['attributes']['name']
                
                # Get instances for this report (filtered by date)
                instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                inst_response = self.requestor._asc_request('GET', instances_url, timeout=30)
                
                if inst_response is None or inst_response.status_code != 200:
                    continue
                
                instances = inst_response.json().get('data', [])
                
                # Filter instances by target_date
                matching_instances = []
                for instance in instances:
                    instance_attrs = instance.get('attributes', {})
                    processing_date = instance_attrs.get('processingDate')
                    
                    # Parse date - handle different formats
                    try:
                        if processing_date:
                            instance_date = processing_date.split('T')[0]  # Extract YYYY-MM-DD
                            if instance_date == target_date:
                                matching_instances.append(instance)
                    except:
                        pass
                
                if matching_instances:
                    logger.info(f"      Found {len(matching_instances)} instances for {report_name}")
                
                # Download data from matching instances
                for instance in matching_instances:
                    instance_id = instance['id']
                    files, rows = self._download_instance_data(
                        instance_id, app_id, report_name, target_date
                    )
                    
                    if files > 0:
                        result['files'] += files
                        result['rows'] += rows
            
            if result['files'] > 0:
                result['success'] = True
                logger.info(f"   ✅ Extracted {result['files']} files, {result['rows']} rows for {target_date}")
            else:
                result['success'] = True  # Not an error, just no data for this date
                logger.info(f"   ℹ️ No data available for {target_date}")
            
            return result
            
        except Exception as e:
            logger.error(f"   ❌ Exception extracting data: {e}")
            result['errors'].append(str(e))
            import traceback
            traceback.print_exc()
            return result
    
    # =========================================================================
    # TRANSFORM PHASE - CSV to Parquet
    # =========================================================================
    
    def transform_to_parquet(self, target_date: str) -> Dict:
        """Transform raw CSV to curated Parquet for a date"""
        result = {'files': 0, 'rows': 0, 'by_type': {}}
        
        data_types = ['downloads', 'engagement', 'sessions', 'installs', 'purchases']
        
        for data_type in data_types:
            type_result = self._curate_data_type(data_type, target_date)
            result['by_type'][data_type] = type_result
            result['files'] += type_result['files']
            result['rows'] += type_result['rows']
        
        return result
    
    def _curate_data_type(self, data_type: str, target_date: str) -> Dict:
        """Curate a specific data type for a date"""
        result = {'files': 0, 'rows': 0}
        
        prefix = f'appstore/raw/{data_type}/dt={target_date}/'
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
            app_prefixes = response.get('CommonPrefixes', [])
        except Exception:
            return result
        
        for app_prefix in app_prefixes:
            app_path = app_prefix['Prefix']
            if 'app_id=' not in app_path:
                continue
            
            app_id = app_path.split('app_id=')[1].rstrip('/')
            
            try:
                curated_rows = self._curate_app_data(data_type, app_id, target_date)
                if curated_rows > 0:
                    result['files'] += 1
                    result['rows'] += curated_rows
            except Exception as e:
                logger.debug(f"Curate error {data_type}/{app_id}: {e}")
        
        if result['files'] > 0:
            logger.info(f"   📊 {data_type}: {result['files']} apps, {result['rows']:,} rows")
        
        return result
    
    def _curate_app_data(self, data_type: str, app_id: str, target_date: str) -> int:
        """Curate data for a specific app/type/date"""
        prefix = f'appstore/raw/{data_type}/dt={target_date}/app_id={app_id}/'
        
        dfs = []
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        
        for obj in response.get('Contents', []):
            if not obj['Key'].endswith('.csv'):
                continue
            try:
                csv_response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                content = csv_response['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
                if not df.empty:
                    dfs.append(df)
            except Exception:
                continue
        
        if not dfs:
            return 0
        
        combined = pd.concat(dfs, ignore_index=True)
        curated = self._transform_dataframe(data_type, combined, app_id, target_date)
        
        if curated is None or curated.empty:
            return 0
        
        # Save as Parquet
        output_key = f'appstore/curated/{data_type}/dt={target_date}/app_id={app_id}/data.parquet'
        buffer = io.BytesIO()
        curated.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())
        return len(curated)
    
    def _transform_dataframe(self, data_type: str, df: pd.DataFrame, app_id: str, target_date: str) -> Optional[pd.DataFrame]:
        """Transform raw DataFrame to curated schema with proper deduplication"""
        try:
            # First, deduplicate raw data (Apple sometimes provides overlapping segments)
            df = df.drop_duplicates()
            
            if data_type == 'downloads':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'total_downloads': pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64'),
                    'download_type': df.get('Download Type', ''),
                    'source_type': df.get('Source Type', ''),
                    'device': df.get('Device', ''),
                    'platform_version': df.get('Platform Version', ''),
                    'app_id_part': int(app_id),
                    'dt': target_date
                })
                curated = curated[curated['total_downloads'] > 0]
                # Aggregate by all dimensions, sum metrics
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'download_type', 
                              'source_type', 'device', 'platform_version', 'app_id_part', 'dt']
                return curated.groupby(group_cols, as_index=False).agg({'total_downloads': 'sum'})
                
            elif data_type == 'engagement':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'impressions': pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64'),
                    'source_type': df.get('Source Type', ''),
                    'device': df.get('Device', ''),
                    'app_id_part': int(app_id),
                    'dt': target_date
                })
                curated = curated[curated['impressions'] > 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'source_type', 'device', 'app_id_part', 'dt']
                return curated.groupby(group_cols, as_index=False).agg({'impressions': 'sum'})
                
            elif data_type == 'sessions':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'sessions': pd.to_numeric(df.get('Sessions', df.get('Counts', 0)), errors='coerce').fillna(0).astype('int64'),
                    'device': df.get('Device', ''),
                    'app_id_part': int(app_id),
                    'dt': target_date
                })
                curated = curated[curated['sessions'] > 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'device', 'app_id_part', 'dt']
                return curated.groupby(group_cols, as_index=False).agg({'sessions': 'sum'})
                
            elif data_type == 'installs':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'counts': pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64'),
                    'event': df.get('Event', ''),
                    'device': df.get('Device', ''),
                    'app_id_part': int(app_id),
                    'dt': target_date
                })
                curated = curated[curated['counts'] > 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'event', 'device', 'app_id_part', 'dt']
                return curated.groupby(group_cols, as_index=False).agg({'counts': 'sum'})
                
            elif data_type == 'purchases':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'purchases': pd.to_numeric(df.get('Purchases', df.get('Counts', 0)), errors='coerce').fillna(0).astype('int64'),
                    'proceeds_usd': pd.to_numeric(df.get('Proceeds in USD', 0), errors='coerce').fillna(0.0),
                    'device': df.get('Device', ''),
                    'app_id_part': int(app_id),
                    'dt': target_date
                })
                curated = curated[curated['purchases'] > 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'device', 'app_id_part', 'dt']
                return curated.groupby(group_cols, as_index=False).agg({'purchases': 'sum', 'proceeds_usd': 'sum'})
            
            return None
        except Exception as e:
            logger.debug(f"Transform error for {data_type}: {e}")
            return None
    
    # =========================================================================
    # LOAD PHASE - Refresh Athena
    # =========================================================================
    
    def refresh_athena_partitions(self) -> Dict:
        """Refresh all Athena table partitions"""
        result = {'tables_refreshed': 0, 'errors': []}
        
        # Only refresh tables that we actually curate
        # Note: curated_reviews removed since 'reviews' is not in transform data_types
        tables = [
            'curated_downloads', 'curated_engagement', 'curated_sessions',
            'curated_installs', 'curated_purchases'
        ]
        
        logger.info("🔄 Refreshing Athena partitions...")
        
        for table in tables:
            try:
                self.athena.start_query_execution(
                    QueryString=f'MSCK REPAIR TABLE appstore.{table}',
                    QueryExecutionContext={'Database': 'appstore'},
                    ResultConfiguration={'OutputLocation': self.athena_output}
                )
                result['tables_refreshed'] += 1
            except Exception as e:
                result['errors'].append(f"{table}: {str(e)}")
        
        logger.info(f"   ✅ Refreshed {result['tables_refreshed']} tables")
        return result
    
    # =========================================================================
    # MAIN RUN METHOD
    # =========================================================================
    
    def run(self, target_date: Optional[str] = None, app_id: Optional[str] = None,
            backfill_days: int = 0, onetime: bool = False, start_date: Optional[str] = None,
            end_date: Optional[str] = None) -> Dict:
        """
        Run the complete ETL pipeline
        
        Args:
            target_date: Single date for ONGOING mode (YYYY-MM-DD)
            app_id: Specific app ID to process
            backfill_days: Number of days to backfill (ONGOING mode)
            onetime: Enable ONE_TIME_SNAPSHOT mode
            start_date: Start date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)
            end_date: End date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)
        """
        print("=" * 80)
        print("🍎 APPLE ANALYTICS - UNIFIED ETL PIPELINE")
        print("=" * 80)
        
        app_ids = self.get_app_ids(app_id)
        
        # ONE_TIME_SNAPSHOT MODE
        if onetime:
            if not start_date or not end_date:
                raise ValueError("ONE_TIME_SNAPSHOT mode requires --start-date and --end-date")
            
            print(f"📊 Mode: ONE_TIME_SNAPSHOT (Bulk Backfill)")
            print(f"📅 Date Range: {start_date} to {end_date}")
            print(f"📱 Apps: {len(app_ids)}")
            print("=" * 80)
            
            # Store date range info in results
            self.results['mode'] = 'ONE_TIME_SNAPSHOT'
            self.results['start_date'] = start_date
            self.results['end_date'] = end_date
            
            dates = self.generate_date_range(start_date, end_date)
            
            # Create requests and extract for each app
            for aid in app_ids:
                request_id = self.create_onetime_request_for_range(aid, start_date, end_date)
                if not request_id:
                    logger.error(f"Failed to create request for app {aid}, skipping")
                    continue
                
                # EXTRACT phase - process each date in range
                print(f"\n{'='*60}")
                print(f"🔄 PHASE 1: EXTRACT - App {aid}")
                print(f"{'='*60}")
                
                for date_str in dates:
                    try:
                        result = self._extract_onetime_data(aid, request_id, date_str)
                        self.results['apps_processed'] += 1
                        if result['success']:
                            self.results['apps_successful'] += 1
                            self.results['files_extracted'] += result['files']
                    except Exception as e:
                        logger.error(f"Error extracting {aid}/{date_str}: {e}")
                        self.results['errors'].append(f"{aid}/{date_str}: {str(e)}")
            
            # TRANSFORM phase - process all dates in range
            print(f"\n{'='*60}")
            print(f"🔄 PHASE 2: TRANSFORM")
            print(f"{'='*60}")
            
            for date_str in dates:
                transform_result = self.transform_to_parquet(date_str)
                self.results['files_curated'] += transform_result['files']
                self.results['total_rows'] += transform_result['rows']
        
        # ONGOING MODE (DEFAULT)
        else:
            print(f"📊 Mode: ONGOING (Daily Automation)")
            
            # Determine dates
            if target_date:
                base_date = datetime.strptime(target_date, '%Y-%m-%d')
            else:
                base_date = datetime.now(timezone.utc) - timedelta(days=1)
            
            if backfill_days > 0:
                dates = [(base_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(backfill_days)]
            else:
                dates = [base_date.strftime('%Y-%m-%d')]
            
            print(f"📅 Dates: {len(dates)} ({dates[-1]} to {dates[0]})")
            print(f"📱 Apps: {len(app_ids)}")
            print("=" * 80)
            
            self.results['mode'] = 'ONGOING'
            
            # Process each date
            for date_str in dates:
                print(f"\n{'='*60}")
                print(f"📆 Processing: {date_str}")
                print(f"{'='*60}")
                
                # EXTRACT
                print("\n🔄 PHASE 1: EXTRACT")
                for aid in app_ids:
                    result = self.extract_app_data(aid, date_str)
                    self.results['apps_processed'] += 1
                    if result['success']:
                        self.results['apps_successful'] += 1
                        self.results['files_extracted'] += result['files']
                
                # TRANSFORM
                print("\n🔄 PHASE 2: TRANSFORM")
                transform_result = self.transform_to_parquet(date_str)
                self.results['files_curated'] += transform_result['files']
                self.results['total_rows'] += transform_result['rows']
        
        # LOAD
        print("\n🔄 PHASE 3: LOAD")
        self.refresh_athena_partitions()
        
        # Summary
        self._print_summary()
        self._save_results()
        
        return self.results
    
    def _print_summary(self):
        print("\n" + "=" * 80)
        print("📊 ETL SUMMARY")
        print("=" * 80)
        print(f"Apps Processed:  {self.results['apps_processed']}")
        print(f"Apps Successful: {self.results['apps_successful']}")
        print(f"Files Extracted: {self.results['files_extracted']}")
        print(f"Files Curated:   {self.results['files_curated']}")
        print(f"Total Rows:      {self.results['total_rows']:,}")
        print("=" * 80)
    
    def _save_results(self):
        self.results['end_time'] = datetime.now(timezone.utc).isoformat()
        results_file = os.path.join(SCRIPT_DIR, 'logs', f'unified_etl_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"Results saved to: {results_file}")


def main():
    parser = argparse.ArgumentParser(description='Apple Analytics Unified ETL - ONGOING and ONE_TIME_SNAPSHOT modes')
    
    # ONGOING mode arguments
    parser.add_argument('--date', type=str, help='Target date for ONGOING mode (YYYY-MM-DD)')
    parser.add_argument('--app-id', type=str, help='Specific app ID')
    parser.add_argument('--backfill', action='store_true', help='Backfill mode (ONGOING)')
    parser.add_argument('--days', type=int, default=30, help='Days to backfill (ONGOING mode)')
    
    # ONE_TIME_SNAPSHOT mode arguments
    parser.add_argument('--onetime', action='store_true', help='Enable ONE_TIME_SNAPSHOT mode (requires --start-date and --end-date)')
    parser.add_argument('--start-date', type=str, help='Start date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for ONE_TIME_SNAPSHOT (YYYY-MM-DD)')
    
    # Other arguments
    parser.add_argument('--transform-only', action='store_true', help='Only run transform phase (skip extract)')
    parser.add_argument('--load-only', action='store_true', help='Only run load phase (refresh Athena partitions)')
    
    args = parser.parse_args()
    
    etl = UnifiedETL()
    
    if args.transform_only:
        # Transform-only mode
        target_date = args.date or (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"🔄 TRANSFORM-ONLY MODE for {target_date}")
        result = etl.transform_to_parquet(target_date)
        print(f"✅ Curated {result['files']} files with {result['rows']:,} rows")
        print("\n🔄 Refreshing Athena partitions...")
        etl.refresh_athena_partitions()
        print("✅ Done!")
    elif args.load_only:
        # Load-only mode
        print("🔄 LOAD-ONLY MODE - Refreshing Athena partitions...")
        etl.refresh_athena_partitions()
        print("✅ Done!")
    else:
        # Full ETL mode
        etl.run(
            target_date=args.date,
            app_id=args.app_id,
            backfill_days=args.days if args.backfill else 0,
            onetime=args.onetime,
            start_date=args.start_date,
            end_date=args.end_date
        )


if __name__ == '__main__':
    main()
