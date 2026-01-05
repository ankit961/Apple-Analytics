#!/usr/bin/env python3
"""
Apple Analytics - Unified ONE_TIME_SNAPSHOT ETL Pipeline
=========================================================

This script provides complete ETL for ONE_TIME_SNAPSHOT requests:
1. EXTRACT: Create/reuse ONE_TIME_SNAPSHOT requests for date ranges
2. TRANSFORM: Convert raw CSV/JSON to optimized Parquet
3. LOAD: Refresh Athena table partitions with deduplicate logic

Use Cases:
- Backfill historical data (e.g., last 30 days)
- One-time data extraction for specific date range
- Bulk recovery of missing data
- Export data for specific period

Usage:
    python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-30
    python3 unified_onetime_etl.py --start-date 2025-12-01 --end-date 2025-12-05 --app-id 1506886061
    python3 unified_onetime_etl.py --backfill --days 30  # Last 30 days
    python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-30 --app-id 1506886061 --parallel 8

ONE_TIME_SNAPSHOT Request Flow:
- Creates request with start_date and end_date
- Apple processes and provides all data for date range
- Reports available with instances for each date
- Save request ID to registry for future reference
- Extract all data, transform, and load to Athena
- Deduplication to avoid re-inserting data into Athena
"""

import os
import sys
import io
import json
import gzip
import argparse
import logging
import time
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        logging.FileHandler(os.path.join(SCRIPT_DIR, 'logs', f'unified_onetime_etl_{datetime.now().strftime("%Y%m%d")}.log'))
    ]
)
logger = logging.getLogger(__name__)

# Import Apple Analytics client
from src.extract.apple_analytics_client import AppleAnalyticsRequestor


class UnifiedONETIMEETL:
    """
    Unified ETL Pipeline for Apple Analytics ONE_TIME_SNAPSHOT Requests
    
    Complete flow:
    1. EXTRACT: Creates ONE_TIME_SNAPSHOT request for date range, saves to registry
    2. TRANSFORM: Converts raw CSV to Parquet with deduplication
    3. LOAD: Refreshes Athena partitions with conflict handling
    """
    
    def __init__(self):
        self.requestor = AppleAnalyticsRequestor()
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.athena = boto3.client('athena', region_name='us-east-1')
        self.glue = boto3.client('glue', region_name='us-east-1')
        self.bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        self.athena_output = os.getenv('ATHENA_OUTPUT', 's3://skidos-apptrack/Athena-Output/')
        self.db_name = 'curated'
        
        # Results tracking
        self.results = {
            'start_time': datetime.now(timezone.utc).isoformat(),
            'start_date': None,
            'end_date': None,
            'request_type': 'ONE_TIME_SNAPSHOT',
            'apps_processed': 0,
            'apps_successful': 0,
            'files_extracted': 0,
            'files_curated': 0,
            'total_rows_processed': 0,
            'total_rows_loaded': 0,
            'request_ids': {},
            'errors': []
        }
    
    def get_app_ids(self, specific_app_id: Optional[str] = None) -> List[str]:
        """Get list of app IDs to process"""
        if specific_app_id:
            return [specific_app_id]
        
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        
        # Default test app
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
    
    # =========================================================================
    # EXTRACT PHASE - ONE_TIME_SNAPSHOT requests
    # =========================================================================
    
    def _validate_request_is_available(self, request_id: str) -> bool:
        """Check if a request ID is still valid and accessible"""
        try:
            status_url = f"{self.requestor.api_base}/analyticsRequests/{request_id}"
            resp = self.requestor._asc_request('GET', status_url, timeout=30)
            
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
            import boto3
            s3 = boto3.client('s3', region_name='us-east-1')
            reg_key = f"analytics_requests/registry/app_id={app_id}/one_time_snapshot.json"
            
            existing_request_id = None
            try:
                resp = s3.get_object(Bucket=self.bucket, Key=reg_key)
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
    
    def extract_app_data_onetime(self, app_id: str, request_id: str, target_date: str) -> Dict:
        """
        Extract data from ONE_TIME_SNAPSHOT request for specific date
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
            
            if response.status_code != 200:
                result['errors'].append(f"Failed to get reports: {response.status_code}")
                logger.warning(f"   ⚠️ Failed to get reports for {request_id}: {response.status_code}")
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
                
                if inst_response.status_code != 200:
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
                    files, rows = self._download_instance_data_onetime(
                        app_id, report_name, instance_id, target_date
                    )
                    
                    if files > 0:
                        result['files'] += files
                        result['rows'] += rows
            
            if result['files'] > 0:
                result['success'] = True
                logger.info(f"   ✅ Extracted {result['files']} files, {result['rows']} rows")
            
            return result
            
        except Exception as e:
            logger.error(f"   ❌ Exception extracting data: {e}")
            result['errors'].append(str(e))
            return result
    
    def _download_instance_data_onetime(self, app_id: str, report_name: str, 
                                        instance_id: str, target_date: str) -> Tuple[int, int]:
        """Download data from ONE_TIME_SNAPSHOT instance"""
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
        elif 'review' in report_lower:
            return 'reviews'
        else:
            return 'analytics'
    
    # =========================================================================
    # TRANSFORM PHASE - Convert raw CSV to Parquet with deduplication
    # =========================================================================
    
    def transform_app_data(self, app_id: str, date_str: str) -> Dict:
        """Transform raw CSV files to Parquet for curated tables"""
        result = {
            'app_id': app_id,
            'date': date_str,
            'tables': {},
            'success': False,
            'errors': []
        }
        
        try:
            logger.info(f"   🔄 Transforming data for {app_id} on {date_str}")
            
            # Get list of raw files for this date
            prefix = f"appstore/raw/downloads/dt={date_str}/app_id={app_id}/"
            
            try:
                resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                if 'Contents' not in resp:
                    logger.warning(f"   ⚠️ No raw files found for {date_str}")
                    return result
                
                raw_files = [obj['Key'] for obj in resp['Contents']]
                logger.info(f"   Found {len(raw_files)} raw files")
                
                # Process each file
                for raw_file in raw_files:
                    self._transform_file(raw_file, app_id, date_str, result)
                
            except Exception as e:
                logger.warning(f"   List files error: {e}")
                return result
            
            if result['tables']:
                result['success'] = True
                logger.info(f"   ✅ Transformed to {len(result['tables'])} Parquet files")
            
            return result
            
        except Exception as e:
            logger.error(f"   ❌ Transform error: {e}")
            result['errors'].append(str(e))
            return result
    
    def _transform_file(self, raw_file: str, app_id: str, date_str: str, result: Dict):
        """Transform single raw file to Parquet"""
        try:
            # Determine table name
            table_name = self._get_table_name_from_file(raw_file)
            
            # Read raw CSV
            obj = self.s3.get_object(Bucket=self.bucket, Key=raw_file)
            content = obj['Body'].read().decode('utf-8')
            
            # Parse CSV (tab-separated)
            from io import StringIO
            df = pd.read_csv(StringIO(content), sep='\t', on_bad_lines='skip')
            
            if df.empty:
                logger.warning(f"   ⚠️ Empty file: {raw_file}")
                return
            
            # Add metadata columns
            df['app_id'] = int(app_id)
            df['dt'] = date_str
            df['processed_at'] = datetime.now(timezone.utc).isoformat()
            
            # Deduplicate (remove exact duplicates)
            original_rows = len(df)
            df = df.drop_duplicates()
            deduplicated_rows = len(df)
            
            if deduplicated_rows < original_rows:
                logger.info(f"      Deduped: {original_rows} → {deduplicated_rows} rows")
            
            # Save to Parquet in curated location
            parquet_key = f"appstore/curated/{table_name}/dt={date_str}/app_id={app_id}/{os.path.basename(raw_file).replace('.csv', '.parquet')}"
            
            # Convert to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
            parquet_buffer.seek(0)
            
            # Upload to S3
            self.s3.put_object(
                Bucket=self.bucket,
                Key=parquet_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            
            if table_name not in result['tables']:
                result['tables'][table_name] = {'files': 0, 'rows': 0}
            
            result['tables'][table_name]['files'] += 1
            result['tables'][table_name]['rows'] += deduplicated_rows
            
            logger.info(f"      ✅ {table_name}: {deduplicated_rows} rows")
            
        except Exception as e:
            logger.warning(f"   ⚠️ Transform file error: {e}")
    
    def _get_table_name_from_file(self, file_path: str) -> str:
        """Extract table name from file path"""
        if 'download' in file_path.lower():
            return 'downloads'
        elif 'engagement' in file_path.lower() or 'impression' in file_path.lower():
            return 'engagement'
        elif 'session' in file_path.lower():
            return 'sessions'
        elif 'install' in file_path.lower():
            return 'installs'
        elif 'purchase' in file_path.lower() or 'subscription' in file_path.lower():
            return 'purchases'
        elif 'review' in file_path.lower():
            return 'reviews'
        else:
            return 'analytics'
    
    # =========================================================================
    # LOAD PHASE - Refresh Athena partitions with deduplication
    # =========================================================================
    
    def load_to_athena(self, app_id: str, date_str: str, table_names: List[str]) -> Dict:
        """Load curated Parquet files to Athena with deduplication"""
        result = {
            'app_id': app_id,
            'date': date_str,
            'tables_updated': [],
            'success': False,
            'errors': []
        }
        
        try:
            logger.info(f"   🗄️ Loading to Athena for {app_id} on {date_str}")
            
            for table_name in table_names:
                try:
                    # Check if partition exists
                    query = f"""
                    SELECT COUNT(*) as row_count
                    FROM {self.db_name}.{table_name}
                    WHERE app_id = {app_id}
                    AND dt = '{date_str}'
                    """
                    
                    rows_before = self._execute_query(query)
                    
                    # Run MSCK REPAIR TABLE to add new partitions
                    repair_query = f"MSCK REPAIR TABLE {self.db_name}.{table_name}"
                    self._execute_query(repair_query)
                    
                    # Check rows after
                    rows_after = self._execute_query(query)
                    
                    result['tables_updated'].append({
                        'table': table_name,
                        'rows_before': rows_before,
                        'rows_after': rows_after,
                        'rows_added': rows_after - rows_before
                    })
                    
                    logger.info(f"      ✅ {table_name}: {rows_after - rows_before} new rows")
                    
                except Exception as e:
                    logger.warning(f"      ⚠️ Error updating {table_name}: {e}")
                    result['errors'].append(f"{table_name}: {str(e)}")
            
            if result['tables_updated']:
                result['success'] = True
            
            return result
            
        except Exception as e:
            logger.error(f"   ❌ Load error: {e}")
            result['errors'].append(str(e))
            return result
    
    def _execute_query(self, query_string: str) -> int:
        """Execute Athena query and return row count or 0"""
        try:
            response = self.athena.start_query_execution(
                QueryString=query_string,
                ResultConfiguration={'OutputLocation': self.athena_output},
                WorkGroup='primary'
            )
            
            query_id = response['QueryExecutionId']
            
            # Wait for completion
            for _ in range(30):
                status = self.athena.get_query_execution(QueryExecutionId=query_id)
                state = status['QueryExecution']['Status']['State']
                
                if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(1)
            
            if state == 'SUCCEEDED':
                results = self.athena.get_query_results(QueryExecutionId=query_id)
                if 'ResultSet' in results and len(results['ResultSet']['Rows']) > 1:
                    try:
                        return int(results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
                    except:
                        return 0
            
            return 0
            
        except Exception as e:
            logger.warning(f"Query error: {e}")
            return 0
    
    # =========================================================================
    # ORCHESTRATION
    # =========================================================================
    
    def run_onetime_etl(self, start_date: str, end_date: str, app_ids: Optional[List[str]] = None, 
                        parallel: int = 1) -> Dict:
        """
        Run complete ONE_TIME_SNAPSHOT ETL pipeline
        """
        logger.info("=" * 70)
        logger.info("🚀 STARTING ONE_TIME_SNAPSHOT ETL PIPELINE")
        logger.info(f"   Date Range: {start_date} → {end_date}")
        logger.info(f"   Apps: {len(app_ids or [])} | Parallel: {parallel}")
        logger.info("=" * 70)
        
        self.results['start_date'] = start_date
        self.results['end_date'] = end_date
        
        if not app_ids:
            app_ids = self.get_app_ids()
        
        # Generate date list
        dates = self.generate_date_range(start_date, end_date)
        logger.info(f"📅 Processing {len(dates)} dates")
        
        # Process each app
        for app_id in app_ids:
            logger.info(f"\n{'=' * 70}")
            logger.info(f"📱 APP: {app_id}")
            logger.info(f"{'=' * 70}")
            
            self.results['apps_processed'] += 1
            
            # Create ONE_TIME_SNAPSHOT request for entire date range
            request_id = self.create_onetime_request_for_range(app_id, start_date, end_date)
            if not request_id:
                continue
            
            # Extract, transform, load for each date
            app_success = True
            
            for target_date in dates:
                try:
                    logger.info(f"\n📅 {target_date}")
                    
                    # EXTRACT
                    extract_result = self.extract_app_data_onetime(app_id, request_id, target_date)
                    if not extract_result['success']:
                        logger.warning(f"   ⚠️ No data extracted")
                        continue
                    
                    self.results['files_extracted'] += extract_result['files']
                    self.results['total_rows_processed'] += extract_result['rows']
                    
                    # TRANSFORM
                    transform_result = self.transform_app_data(app_id, target_date)
                    if not transform_result['success']:
                        logger.warning(f"   ⚠️ Transform failed")
                        continue
                    
                    table_names = list(transform_result['tables'].keys())
                    self.results['files_curated'] += sum(t['files'] for t in transform_result['tables'].values())
                    
                    # LOAD
                    load_result = self.load_to_athena(app_id, target_date, table_names)
                    if load_result['success']:
                        for update in load_result['tables_updated']:
                            self.results['total_rows_loaded'] += update['rows_added']
                    
                except Exception as e:
                    logger.error(f"   ❌ Error processing {target_date}: {e}")
                    app_success = False
                    self.results['errors'].append(f"{app_id}/{target_date}: {str(e)}")
            
            if app_success:
                self.results['apps_successful'] += 1
        
        # Summary
        self._print_summary()
        return self.results
    
    def _print_summary(self):
        """Print ETL summary"""
        logger.info(f"\n{'=' * 70}")
        logger.info("📊 ONE_TIME_SNAPSHOT ETL SUMMARY")
        logger.info(f"{'=' * 70}")
        logger.info(f"✅ Apps Processed: {self.results['apps_successful']}/{self.results['apps_processed']}")
        logger.info(f"📥 Files Extracted: {self.results['files_extracted']}")
        logger.info(f"📦 Files Curated: {self.results['files_curated']}")
        logger.info(f"📊 Rows Processed: {self.results['total_rows_processed']:,}")
        logger.info(f"🗄️  Rows Loaded to Athena: {self.results['total_rows_loaded']:,}")
        
        if self.results['errors']:
            logger.warning(f"\n⚠️  Errors ({len(self.results['errors'])}):")
            for error in self.results['errors']:
                logger.warning(f"   - {error}")
        
        logger.info(f"{'=' * 70}\n")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='ONE_TIME_SNAPSHOT ETL for Apple Analytics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill last 30 days
  python3 unified_onetime_etl.py --backfill --days 30
  
  # Specific date range
  python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-30
  
  # Single app for date range
  python3 unified_onetime_etl.py --start-date 2025-12-01 --end-date 2025-12-05 --app-id 1506886061
  
  # Parallel processing
  python3 unified_onetime_etl.py --start-date 2025-11-01 --end-date 2025-11-30 --parallel 8
        """
    )
    
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--backfill', action='store_true', help='Backfill mode')
    parser.add_argument('--days', type=int, default=30, help='Number of days to backfill')
    parser.add_argument('--app-id', type=str, help='Specific app ID')
    parser.add_argument('--parallel', type=int, default=1, help='Parallel threads')
    
    args = parser.parse_args()
    
    # Determine date range
    if args.backfill:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.days + 1)).strftime('%Y-%m-%d')
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        logger.error("❌ Provide --start-date & --end-date or use --backfill")
        return 1
    
    # Run ETL
    try:
        etl = UnifiedONETIMEETL()
        app_ids = [args.app_id] if args.app_id else None
        results = etl.run_onetime_etl(start_date, end_date, app_ids, args.parallel)
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"onetime_etl_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"📄 Results saved: {results_file}")
        
        return 0 if results['apps_successful'] > 0 else 1
        
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
