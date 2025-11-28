#!/usr/bin/env python3
"""
Apple Analytics - Unified Production ETL Pipeline
==================================================

This is the SINGLE script that handles the complete ETL pipeline:
1. EXTRACT: Fetch data from Apple Analytics API (ONGOING requests)
2. TRANSFORM: Convert raw CSV/JSON to optimized Parquet
3. LOAD: Refresh Athena table partitions

Usage:
    python3 unified_etl.py                    # Process yesterday's data for all apps
    python3 unified_etl.py --date 2025-11-27  # Process specific date
    python3 unified_etl.py --app-id 1506886061  # Process specific app
    python3 unified_etl.py --backfill --days 30  # Backfill last 30 days
"""

import os
import sys
import io
import json
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
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))
load_dotenv(os.path.join(os.path.dirname(SCRIPT_DIR), '.env'))

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
    
    Handles:
    - EXTRACT: Uses ONGOING requests (not ONE_TIME_SNAPSHOT) to avoid 409 conflicts
    - TRANSFORM: Converts raw CSV/JSON to Parquet
    - LOAD: Refreshes Athena partitions
    """
    
    def __init__(self):
        self.requestor = AppleAnalyticsRequestor()
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.athena = boto3.client('athena', region_name='us-east-1')
        self.bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        self.athena_output = os.getenv('ATHENA_OUTPUT', 's3://skidos-apptrack/Athena-Output/')
        
        # Ensure logs directory exists
        os.makedirs(os.path.join(SCRIPT_DIR, 'logs'), exist_ok=True)
        
        # Results tracking
        self.results = {
            'start_time': datetime.now(timezone.utc).isoformat(),
            'apps_processed': 0,
            'apps_successful': 0,
            'files_extracted': 0,
            'files_curated': 0,
            'total_rows': 0,
            'errors': [],
            'by_data_type': {}
        }
    
    def get_app_ids(self, specific_app_id: Optional[str] = None) -> List[str]:
        """Get list of app IDs to process"""
        if specific_app_id:
            return [specific_app_id]
        
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        
        # Fallback
        return ['1506886061']
    
    # =========================================================================
    # EXTRACT PHASE
    # =========================================================================
    
    def extract_app_data(self, app_id: str, target_date: str) -> Dict:
        """Extract all data for a single app and date"""
        result = {
            'app_id': app_id,
            'date': target_date,
            'files': 0,
            'rows': 0,
            'success': False,
            'errors': []
        }
        
        try:
            logger.info(f"📱 Extracting app {app_id} for {target_date}")
            
            # Get or create ONGOING request
            request_id = self.requestor.create_or_reuse_ongoing_request(app_id)
            if not request_id:
                result['errors'].append(f"Failed to get request for {app_id}")
                return result
            
            logger.info(f"   Using request: {request_id}")
            
            # Get reports
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
                
                # Get instances
                instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                params = {'filter[processingDate]': target_date}
                inst_response = self.requestor._asc_request('GET', instances_url, params=params, timeout=60)
                
                if inst_response.status_code != 200:
                    continue
                
                instances = inst_response.json().get('data', [])
                
                for instance in instances:
                    rows = self._download_instance(instance, app_id, report_name, target_date)
                    if rows > 0:
                        result['files'] += 1
                        result['rows'] += rows
            
            result['success'] = result['files'] > 0
            if result['success']:
                logger.info(f"   ✅ Extracted {result['files']} files, {result['rows']:,} rows")
            else:
                logger.warning(f"   ⚠️ No data found for {target_date}")
                
        except Exception as e:
            result['errors'].append(str(e))
            logger.error(f"   ❌ Error: {e}")
        
        return result
    
    def _download_instance(self, instance: Dict, app_id: str, report_name: str, target_date: str) -> int:
        """Download a report instance to S3"""
        try:
            instance_id = instance['id']
            
            # Get segments
            segments_url = f"{self.requestor.api_base}/analyticsReportInstances/{instance_id}/segments"
            seg_response = self.requestor._asc_request('GET', segments_url, timeout=60)
            
            if seg_response.status_code != 200:
                return 0
            
            segments = seg_response.json().get('data', [])
            total_rows = 0
            
            for segment in segments:
                download_url = segment['attributes'].get('url')
                if not download_url:
                    continue
                
                # Download the data
                import requests
                download_resp = requests.get(download_url, timeout=120)
                if download_resp.status_code != 200:
                    continue
                
                content = download_resp.content.decode('utf-8')
                if not content.strip():
                    continue
                
                # Determine data type from report name
                data_type = self._get_data_type(report_name)
                
                # Save to S3
                s3_key = f"appstore/raw/{data_type}/dt={target_date}/app_id={app_id}/{report_name}_{instance_id}.csv"
                
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=s3_key,
                    Body=content.encode('utf-8'),
                    ContentType='text/csv'
                )
                
                row_count = len(content.strip().split('\n')) - 1
                total_rows += row_count
            
            return total_rows
            
        except Exception as e:
            logger.debug(f"Download error: {e}")
            return 0
    
    def _get_data_type(self, report_name: str) -> str:
        """Map report name to data type"""
        name_lower = report_name.lower()
        if 'download' in name_lower or 'redownload' in name_lower:
            return 'downloads'
        elif 'session' in name_lower:
            return 'sessions'
        elif 'install' in name_lower or 'deletion' in name_lower:
            return 'installs'
        elif 'purchase' in name_lower or 'subscription' in name_lower or 'iap' in name_lower:
            return 'purchases'
        elif 'impression' in name_lower or 'page_view' in name_lower or 'engagement' in name_lower:
            return 'engagement'
        elif 'review' in name_lower or 'rating' in name_lower:
            return 'reviews'
        else:
            return 'analytics'
    
    # =========================================================================
    # TRANSFORM PHASE
    # =========================================================================
    
    def transform_to_parquet(self, target_date: str) -> Dict:
        """Transform raw CSV to curated Parquet for a date"""
        result = {'files': 0, 'rows': 0, 'by_type': {}}
        
        data_types = ['downloads', 'engagement', 'sessions', 'installs', 'purchases', 'reviews']
        
        for data_type in data_types:
            type_result = self._curate_data_type(data_type, target_date)
            result['by_type'][data_type] = type_result
            result['files'] += type_result['files']
            result['rows'] += type_result['rows']
        
        return result
    
    def _curate_data_type(self, data_type: str, target_date: str) -> Dict:
        """Curate a specific data type for a date"""
        result = {'files': 0, 'rows': 0, 'apps': []}
        
        # List apps with raw data
        prefix = f'appstore/raw/{data_type}/dt={target_date}/'
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
            app_prefixes = response.get('CommonPrefixes', [])
        except Exception as e:
            logger.warning(f"Error listing {prefix}: {e}")
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
                    result['apps'].append(app_id)
            except Exception as e:
                logger.warning(f"Error curating {data_type}/{app_id}: {e}")
        
        if result['files'] > 0:
            logger.info(f"   📊 {data_type}: {result['files']} apps, {result['rows']:,} rows")
        
        return result
    
    def _curate_app_data(self, data_type: str, app_id: str, target_date: str) -> int:
        """Curate data for a specific app/type/date"""
        prefix = f'appstore/raw/{data_type}/dt={target_date}/app_id={app_id}/'
        
        # Read all CSV files
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
        
        # Transform based on data type
        curated = self._transform_dataframe(data_type, combined, app_id, target_date)
        
        if curated is None or curated.empty:
            return 0
        
        # Save as Parquet
        output_key = f'appstore/curated/{data_type}/dt={target_date}/app_id={app_id}/data.parquet'
        buffer = io.BytesIO()
        curated.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=output_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        return len(curated)
    
    def _transform_dataframe(self, data_type: str, df: pd.DataFrame, app_id: str, target_date: str) -> Optional[pd.DataFrame]:
        """Transform raw DataFrame to curated schema"""
        try:
            if data_type == 'downloads':
                curated = pd.DataFrame()
                curated['metric_date'] = pd.to_datetime(df['Date']).dt.date
                curated['app_name'] = df['App Name']
                curated['app_id'] = pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
                curated['territory'] = df['Territory']
                curated['total_downloads'] = pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64')
                curated['download_type'] = df.get('Download Type', '')
                curated['source_type'] = df.get('Source Type', '')
                curated['device'] = df.get('Device', '')
                curated['platform_version'] = df.get('Platform Version', '')
                curated['page_type'] = df.get('Page Type', '')
                curated = curated[curated['total_downloads'] > 0]
                
            elif data_type == 'engagement':
                curated = pd.DataFrame()
                curated['metric_date'] = pd.to_datetime(df['Date']).dt.date
                curated['app_name'] = df['App Name']
                curated['app_id'] = pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
                curated['source_type'] = df.get('Source Type', '')
                curated['page_type'] = df.get('Page Type', '')
                curated['territory'] = df['Territory']
                curated['event_type'] = df.get('Event', '')
                curated['engagement_type'] = df.get('Engagement Type', '')
                curated['impressions'] = pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64')
                curated['impressions_unique'] = pd.to_numeric(df.get('Unique Counts', 0), errors='coerce').fillna(0).astype('int64')
                curated['device'] = df.get('Device', '')
                curated['platform_version'] = df.get('Platform Version', '')
                curated = curated[(curated['impressions'] > 0) | (curated['impressions_unique'] > 0)]
                
            elif data_type == 'sessions':
                curated = pd.DataFrame()
                curated['metric_date'] = pd.to_datetime(df['Date']).dt.date
                curated['app_name'] = df['App Name']
                curated['app_id'] = pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
                curated['app_version'] = df.get('App Version', '')
                curated['device'] = df.get('Device', '')
                curated['platform_version'] = df.get('Platform Version', '')
                curated['source_type'] = df.get('Source Type', '')
                curated['territory'] = df['Territory']
                curated['sessions'] = pd.to_numeric(df.get('Sessions', df.get('Counts', 0)), errors='coerce').fillna(0).astype('int64')
                curated['total_session_duration'] = pd.to_numeric(df.get('Total Session Duration', 0), errors='coerce').fillna(0).astype('int64')
                curated['unique_devices'] = pd.to_numeric(df.get('Unique Devices', 0), errors='coerce').fillna(0).astype('int64')
                curated = curated[curated['sessions'] > 0]
                
            elif data_type == 'installs':
                curated = pd.DataFrame()
                curated['metric_date'] = pd.to_datetime(df['Date']).dt.date
                curated['app_name'] = df['App Name']
                curated['app_id'] = pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
                curated['event'] = df.get('Event', '')
                curated['download_type'] = df.get('Download Type', '')
                curated['app_version'] = df.get('App Version', '')
                curated['device'] = df.get('Device', '')
                curated['platform_version'] = df.get('Platform Version', '')
                curated['source_type'] = df.get('Source Type', '')
                curated['territory'] = df['Territory']
                curated['counts'] = pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64')
                curated['unique_devices'] = pd.to_numeric(df.get('Unique Devices', 0), errors='coerce').fillna(0).astype('int64')
                curated = curated[curated['counts'] > 0]
                
            elif data_type == 'purchases':
                curated = pd.DataFrame()
                curated['metric_date'] = pd.to_datetime(df['Date']).dt.date
                curated['app_name'] = df['App Name']
                curated['app_id'] = pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
                curated['purchase_type'] = df.get('Purchase Type', '')
                curated['content_name'] = df.get('Content Name', '')
                curated['payment_method'] = df.get('Payment Method', '')
                curated['device'] = df.get('Device', '')
                curated['platform_version'] = df.get('Platform Version', '')
                curated['source_type'] = df.get('Source Type', '')
                curated['territory'] = df['Territory']
                curated['purchases'] = pd.to_numeric(df.get('Purchases', df.get('Counts', 0)), errors='coerce').fillna(0).astype('int64')
                curated['proceeds_usd'] = pd.to_numeric(df.get('Proceeds in USD', 0), errors='coerce').fillna(0.0)
                curated['sales_usd'] = pd.to_numeric(df.get('Sales in USD', 0), errors='coerce').fillna(0.0)
                curated['paying_users'] = pd.to_numeric(df.get('Paying Users', 0), errors='coerce').fillna(0).astype('int64')
                curated = curated[curated['purchases'] > 0]
                
            elif data_type == 'reviews':
                # Reviews might be JSON
                curated = pd.DataFrame()
                curated['review_date'] = pd.to_datetime(df.get('Date', df.get('lastModified', '')), errors='coerce')
                curated['app_name'] = df.get('App Name', df.get('appName', ''))
                curated['app_id'] = pd.to_numeric(df.get('App Apple Identifier', df.get('appId', 0)), errors='coerce').fillna(0).astype('int64')
                curated['territory'] = df.get('Territory', df.get('territory', ''))
                curated['rating'] = pd.to_numeric(df.get('Rating', df.get('rating', 0)), errors='coerce').fillna(0).astype('int64')
                curated['title'] = df.get('Title', df.get('title', ''))
                curated['body'] = df.get('Body', df.get('body', ''))
                curated['reviewer_nickname'] = df.get('Reviewer Nickname', df.get('reviewerNickname', ''))
                curated = curated[curated['app_id'] > 0]
            else:
                return None
            
            # Add partition columns
            curated['app_id_part'] = int(app_id)
            curated['dt'] = target_date
            
            return curated.drop_duplicates()
            
        except Exception as e:
            logger.warning(f"Transform error for {data_type}: {e}")
            return None
    
    # =========================================================================
    # LOAD PHASE
    # =========================================================================
    
    def refresh_athena_partitions(self) -> Dict:
        """Refresh all Athena table partitions"""
        result = {'tables_refreshed': 0, 'errors': []}
        
        tables = [
            'curated_downloads', 'curated_engagement', 'curated_sessions',
            'curated_installs', 'curated_purchases', 'curated_reviews'
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
                logger.info(f"   ✅ {table}")
            except Exception as e:
                result['errors'].append(f"{table}: {str(e)}")
                logger.warning(f"   ❌ {table}: {e}")
        
        return result
    
    # =========================================================================
    # MAIN RUN METHOD
    # =========================================================================
    
    def run(self, 
            target_date: Optional[str] = None,
            app_id: Optional[str] = None,
            backfill_days: int = 0) -> Dict:
        """
        Run the complete ETL pipeline
        
        Args:
            target_date: Specific date (YYYY-MM-DD), defaults to yesterday
            app_id: Specific app ID, defaults to all apps from env
            backfill_days: Number of days to backfill (0 = just target_date)
        """
        print("=" * 80)
        print("🍎 APPLE ANALYTICS - UNIFIED ETL PIPELINE")
        print("=" * 80)
        
        # Determine dates to process
        if target_date:
            base_date = datetime.strptime(target_date, '%Y-%m-%d')
        else:
            base_date = datetime.now(timezone.utc) - timedelta(days=1)
        
        if backfill_days > 0:
            dates = [(base_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(backfill_days)]
        else:
            dates = [base_date.strftime('%Y-%m-%d')]
        
        # Get app IDs
        app_ids = self.get_app_ids(app_id)
        
        print(f"📅 Dates to process: {len(dates)} ({dates[0]} to {dates[-1] if len(dates) > 1 else dates[0]})")
        print(f"📱 Apps to process: {len(app_ids)}")
        print(f"📊 Total jobs: {len(dates) * len(app_ids)}")
        print("=" * 80)
        
        # Process each date
        for date_str in dates:
            print(f"\n{'='*60}")
            print(f"📆 Processing: {date_str}")
            print(f"{'='*60}")
            
            # EXTRACT PHASE
            print("\n🔄 PHASE 1: EXTRACT (Apple API → S3 Raw)")
            extract_results = []
            for aid in app_ids:
                result = self.extract_app_data(aid, date_str)
                extract_results.append(result)
                self.results['apps_processed'] += 1
                if result['success']:
                    self.results['apps_successful'] += 1
                    self.results['files_extracted'] += result['files']
            
            successful = sum(1 for r in extract_results if r['success'])
            print(f"   Extracted: {successful}/{len(app_ids)} apps")
            
            # TRANSFORM PHASE
            print("\n🔄 PHASE 2: TRANSFORM (CSV → Parquet)")
            transform_result = self.transform_to_parquet(date_str)
            self.results['files_curated'] += transform_result['files']
            self.results['total_rows'] += transform_result['rows']
            print(f"   Curated: {transform_result['files']} files, {transform_result['rows']:,} rows")
        
        # LOAD PHASE (once at end)
        print("\n🔄 PHASE 3: LOAD (Refresh Athena)")
        load_result = self.refresh_athena_partitions()
        print(f"   Refreshed: {load_result['tables_refreshed']} tables")
        
        # Final Summary
        self.results['end_time'] = datetime.now(timezone.utc).isoformat()
        self._print_summary()
        self._save_results()
        
        return self.results
    
    def _print_summary(self):
        """Print final summary"""
        print("\n" + "=" * 80)
        print("📊 ETL PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Apps Processed:    {self.results['apps_processed']}")
        print(f"Apps Successful:   {self.results['apps_successful']}")
        print(f"Files Extracted:   {self.results['files_extracted']}")
        print(f"Files Curated:     {self.results['files_curated']}")
        print(f"Total Rows:        {self.results['total_rows']:,}")
        print(f"Errors:            {len(self.results['errors'])}")
        print("=" * 80)
    
    def _save_results(self):
        """Save results to JSON file"""
        results_file = os.path.join(
            SCRIPT_DIR, 'logs',
            f'unified_etl_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        )
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"Results saved to: {results_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Apple Analytics Unified ETL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 unified_etl.py                      # Yesterday's data, all apps
    python3 unified_etl.py --date 2025-11-27    # Specific date
    python3 unified_etl.py --app-id 1506886061  # Specific app
    python3 unified_etl.py --backfill --days 30 # Last 30 days
        """
    )
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--app-id', type=str, help='Specific app ID')
    parser.add_argument('--backfill', action='store_true', help='Enable backfill mode')
    parser.add_argument('--days', type=int, default=30, help='Days to backfill (default: 30)')
    
    args = parser.parse_args()
    
    etl = UnifiedETL()
    
    backfill_days = args.days if args.backfill else 0
    
    etl.run(
        target_date=args.date,
        app_id=args.app_id,
        backfill_days=backfill_days
    )


if __name__ == '__main__':
    main()
