#!/usr/bin/env python3
"""
Apple Analytics - Unified Production ETL Pipeline
==================================================

This is the SINGLE script for complete ETL:
1. EXTRACT: Fetch data from Apple Analytics API (ONGOING requests)
2. TRANSFORM: Convert raw CSV/JSON to optimized Parquet
3. LOAD: Refresh Athena table partitions

Usage:
    python3 unified_etl.py                    # Process yesterday's data for all apps
    python3 unified_etl.py --date 2025-11-27  # Process specific date
    python3 unified_etl.py --app-id 1506886061  # Process specific app
    python3 unified_etl.py --backfill --days 30  # Backfill last 30 days

ONGOING Request Flow:
- Day 1: Creates new ONGOING request, saves to S3 registry
- Day 2+: Reuses same request from registry (ONGOING requests don't expire)
- Apple adds new report instances daily, we fetch by processingDate
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
    
    Complete flow:
    1. EXTRACT: Uses ONGOING requests (created once, reused forever)
    2. TRANSFORM: Converts raw CSV to Parquet
    3. LOAD: Refreshes Athena partitions
    """
    
    # Apple refines data over 3-4 days, so we look back to get the most complete data
    LOOKBACK_DAYS = 5
    
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
            'errors': [], 'reviews_extracted': 0, 'reviews_apps': 0
        }
    
    def get_app_ids(self, specific_app_id: Optional[str] = None) -> List[str]:
        """Get list of app IDs to process"""
        if specific_app_id:
            return [specific_app_id]
        
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        
        return ['1506886061']
    
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
                    
                    # Get instances for this report
                    instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                    inst_response = self.requestor._asc_request('GET', instances_url, timeout=30)
                    
                    if inst_response.status_code != 200:
                        continue
                    
                    instances = inst_response.json().get('data', [])
                    
                    # Download data from each instance
                    for instance in instances:
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
    # EXTRACT REVIEWS - App Store Connect Customer Reviews API
    # =========================================================================

    def extract_reviews(self, app_id: str, target_date: str) -> int:
        """Extract customer reviews from App Store Connect API.

        Uses the same JWT auth as analytics reports.
        Saves reviews as JSON to S3, then curates to parquet.
        Deduplicates by review_id to avoid storing duplicates.

        Returns number of new reviews extracted.
        """
        import json

        logger.info(f"  Extracting reviews for app {app_id}...")

        try:
            url = f"{self.requestor.api_base}/apps/{app_id}/customerReviews"
            params = {
                'sort': '-createdDate',
                'limit': 200,
            }

            all_reviews = []
            pages_fetched = 0
            max_pages = 25  # Safety limit: 25 pages * 200 = 5000 reviews max per run

            while url and pages_fetched < max_pages:
                try:
                    response = self.requestor._asc_request('GET', url, params=params if pages_fetched == 0 else None, timeout=30)

                    if response.status_code != 200:
                        logger.warning(f"  Reviews API returned {response.status_code} for app {app_id}")
                        break

                    data = response.json()
                    reviews = data.get('data', [])
                    included = data.get('included', [])

                    if not reviews:
                        break

                    for review in reviews:
                        attrs = review.get('attributes', {})
                        review_id = review.get('id', '')

                        all_reviews.append({
                            'review_id': review_id,
                            'review_date': attrs.get('createdDate', '')[:10],
                            'rating': attrs.get('rating', 0),
                            'title': attrs.get('title', ''),
                            'review_text': attrs.get('body', ''),
                            'reviewer': attrs.get('reviewerNickname', ''),
                            'territory': attrs.get('territory', ''),
                            'app_version': '',
                            'developer_response': '',
                        })

                    pages_fetched += 1

                    # Get next page URL
                    next_link = data.get('links', {}).get('next')
                    if next_link:
                        url = next_link
                        params = None  # Next page URL includes params
                    else:
                        break

                except Exception as e:
                    logger.warning(f"  Reviews page {pages_fetched} error: {e}")
                    break

            if not all_reviews:
                logger.info(f"  No reviews found for app {app_id}")
                return 0

            logger.info(f"  Fetched {len(all_reviews)} reviews for app {app_id} ({pages_fetched} pages)")

            # Load existing reviews to deduplicate
            existing_ids = set()
            existing_key = f"appstore/curated/reviews/dt={target_date}/app_id_partition={app_id}/data.parquet"
            try:
                resp = self.s3.get_object(Bucket=self.bucket, Key=existing_key)
                import pyarrow.parquet as pq
                existing_df = pq.read_table(io.BytesIO(resp['Body'].read())).to_pandas()
                existing_ids = set(existing_df['review_id'].tolist())
                logger.debug(f"  Found {len(existing_ids)} existing reviews")
            except:
                pass

            # Filter to new reviews only
            new_reviews = [r for r in all_reviews if r['review_id'] not in existing_ids]

            if not new_reviews and existing_ids:
                logger.info(f"  No new reviews (all {len(all_reviews)} already exist)")
                return 0

            # Convert to DataFrame and save as parquet
            # Build DataFrame matching Athena curated_reviews table schema
            df = pd.DataFrame(all_reviews)
            df['app_id'] = pd.to_numeric(app_id, errors='coerce').astype('int64')
            df['app_name'] = ''
            df['app_id_part'] = pd.to_numeric(app_id, errors='coerce').astype('int64')
            df['dt'] = target_date

            # Match existing column names to Athena schema
            df = df.rename(columns={'review_text': 'body', 'reviewer': 'reviewer_nickname'})

            # Convert review_date to proper date type (matching existing schema)
            df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce').dt.date

            # Deduplicate by review_id
            df = df.drop_duplicates(subset=['review_id'], keep='first')

            # Ensure correct types matching Athena table
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0).astype('int64')

            # Select only columns that exist in the Athena table
            table_cols = ['review_id', 'review_date', 'app_name', 'app_id', 'territory',
                          'rating', 'title', 'body', 'app_version', 'developer_response', 'app_id_part']
            for col in table_cols:
                if col not in df.columns:
                    df[col] = '' if col in ['title', 'body', 'app_version', 'developer_response', 'app_name'] else None
            df = df[table_cols]

            # Ensure review_date is date type (not string) for Athena compatibility
            df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce').dt.date

            # Save to S3
            output_key = f"appstore/curated/reviews/dt={target_date}/app_id_partition={app_id}/data.parquet"
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())

            logger.info(f"  Saved {len(df)} reviews ({len(new_reviews)} new) for app {app_id}")
            return len(new_reviews)

        except Exception as e:
            logger.error(f"  Review extraction error for app {app_id}: {e}")
            return 0

    def extract_all_reviews(self, target_date: str, specific_app_id: Optional[str] = None) -> Dict:
        """Extract reviews for all apps"""
        app_ids = self.get_app_ids(specific_app_id)
        result = {'apps': 0, 'new_reviews': 0, 'errors': 0}

        logger.info(f"Extracting reviews for {len(app_ids)} apps...")

        for app_id in app_ids:
            try:
                new_count = self.extract_reviews(app_id, target_date)
                result['apps'] += 1
                result['new_reviews'] += new_count
            except Exception as e:
                result['errors'] += 1
                logger.error(f"  Review extraction failed for {app_id}: {e}")

        logger.info(f"   Reviews: {result['apps']} apps, {result['new_reviews']} new reviews, {result['errors']} errors")
        return result

    # =========================================================================
            # EXTRACT REVIEWS
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
        """Curate a specific data type for a date
        
        NEW APPROACH (based on findings):
        - Apple's ONGOING requests include ALL historical data in the latest processing date
        - Each dt folder contains multiple files, each with 2 days of metric data
        - We read ALL files from the target processing date and deduplicate
        - Then group by metric_date and write separate curated files
        """
        result = {'files': 0, 'rows': 0}
        
        # Find all app_ids that have data for this processing date
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
                # For engagement, use lookback across multiple processing dates
                # because Apple delivers additional segment files in earlier processing dates
                if False and data_type == "engagement":
                    curated_rows = self._curate_engagement_with_lookback(app_id, target_date)
                else:
                    curated_rows = self._curate_app_data_from_processing_date(data_type, app_id, target_date)
                if curated_rows > 0:
                    result['files'] += 1
                    result['rows'] += curated_rows
            except Exception as e:
                logger.debug(f"Curate error {data_type}/{app_id}: {e}")
        
        if result['files'] > 0:
            logger.info(f"   📊 {data_type}: {result['files']} apps, {result['rows']:,} rows")
        return result
    
    def _curate_engagement_with_lookback(self, app_id: str, processing_date: str) -> int:
        """Curate engagement data by combining files across multiple processing dates.

        Apple delivers engagement data incrementally - each processing date folder
        contains segment files, but not all segments appear in every processing date.
        To get the complete picture, we need to read standard files from multiple
        processing dates and combine them.

        Strategy:
        1. Read standard files from current and previous LOOKBACK_DAYS processing dates
        2. Combine all files, drop exact row duplicates
        3. Run through _transform_dataframe (which does SUM aggregation)
        4. Write separate curated files per metric_date
        """
        base_date = datetime.strptime(processing_date, '%Y-%m-%d')
        all_dfs = []
        files_read = 0

        # Read from current and previous processing dates
        for day_offset in range(-self.LOOKBACK_DAYS, 1):
            pd_str = (base_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')
            prefix = f'appstore/raw/engagement/dt={pd_str}/app_id={app_id}/'

            try:
                response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

                for obj in response.get('Contents', []):
                    if not obj['Key'].endswith('.csv'):
                        continue

                    filename = obj['Key'].split('/')[-1].lower()

                    # Only use STANDARD files (not detailed or performance)
                    if 'detailed' in filename or 'performance' in filename:
                        continue

                    try:
                        csv_response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                        content = csv_response['Body'].read().decode('utf-8')
                        df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
                        if df.empty or 'Date' not in df.columns:
                            continue
                        # Filter to rolling 3-day segments only (matches ASC dashboard;
                        # 1-day singletons and long-rollups inflate totals when summed)
                        if df['Date'].nunique() != 3:
                            continue
                        all_dfs.append(df)
                        files_read += 1
                    except Exception as e:
                        logger.debug(f"Error reading {obj['Key']}: {e}")
                        continue

            except Exception as e:
                logger.debug(f"Error listing dt={pd_str}: {e}")
                continue

        if not all_dfs:
            return 0

        # Combine ALL files and remove exact row duplicates
        combined = pd.concat(all_dfs, ignore_index=True)
        combined = combined.drop_duplicates()

        logger.debug(f"      engagement/{app_id}: {files_read} files from {self.LOOKBACK_DAYS+1} processing dates, {len(combined):,} rows after dedup")

        # Transform (this does SUM aggregation by dimension)
        curated = self._transform_dataframe('engagement', combined, app_id, processing_date)

        if curated is None or curated.empty:
            return 0

        # Write separate files for each metric_date
        total_rows = 0
        for metric_date_val, date_group in curated.groupby('dt'):
            output_key = f'appstore/curated/engagement/dt={metric_date_val}/app_id={app_id}/data.parquet'
            buffer = io.BytesIO()
            date_group.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())
            total_rows += len(date_group)

        return total_rows

    # Apple ships each request as many segment CSVs. Across one processing date the
    # same logical row often appears in several segments with conflicting values.
    # "Rolling" segments (3-day for engagement, multi-day for sessions/installs) carry
    # the refined daily numbers that match the App Store Connect dashboard. "Singleton"
    # 1-day and long-rollup segments report extended attribution sums that inflate
    # totals when summed with rolling segments. Filter to rolling segments before
    # transform. Empirically validated 2026-05-21 vs ASC for 3 apps × 3 dates.
    # Empirically validated against Apple UI:
    #   - engagement: 3-day rolling matches impressionsTotal exactly
    #   - sessions/installs: large apps get 6-day rolling, small apps get 3-day
    #     rolling. Both are clean; the 4-day and 23-day are "extended attribution"
    #     versions with inflated cumulative numbers — exclude.
    # Singletons (n=1) and long-rollups (n>=10) always carry the inflated data.
    SEGMENT_FILTER = {}  # span filter removed 2026-07: it dropped apps whose Apple STANDARD report span was off-whitelist (e.g. 1-day-span Hospital); drop_duplicates handles overlaps

    def _curate_app_data_from_processing_date(self, data_type: str, app_id: str, processing_date: str) -> int:
        """Curate ALL metric dates from a single processing date folder

        Key findings from data analysis:
        1. Each processing date folder contains MULTIPLE files (35+ files)
        2. Each file contains 2 days of metric data
        3. Files overlap - same metric date appears in multiple files
        4. We need to read ALL files, deduplicate, and group by metric_date

        Segment filtering (2026-05-21 fix): For each data type we drop segments
        whose date span doesn't match the rolling pattern Apple uses for the UI
        metric. Without this, sessions/installs over-count by 2-100x.
        """
        prefix = f'appstore/raw/{data_type}/dt={processing_date}/app_id={app_id}/'

        all_dfs = []
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        segment_fits = self.SEGMENT_FILTER.get(data_type, lambda n: True)
        kept = skipped = 0

        for obj in response.get('Contents', []):
            if not obj['Key'].endswith('.csv'):
                continue

            filename = obj['Key'].split('/')[-1].lower()

            # Only use STANDARD files (complete data)
            # Skip DETAILED files (only attributed data, ~15% of total)
            if 'detailed' in filename or 'performance' in filename:
                continue

            try:
                csv_response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                content = csv_response['Body'].read().decode('utf-8')
                df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
                if df.empty or 'Date' not in df.columns:
                    continue
                # Drop segments whose date span doesn't match the rolling pattern
                if not segment_fits(df['Date'].nunique()):
                    skipped += 1
                    continue
                all_dfs.append(df)
                kept += 1
            except Exception as e:
                logger.debug(f"Error reading {obj['Key']}: {e}")
                continue

        if not all_dfs:
            return 0

        if skipped:
            logger.debug(f"      {data_type}/{app_id}: kept {kept} rolling segments, skipped {skipped} non-rolling")

        # Combine all files and DEDUPLICATE (critical!)
        combined = pd.concat(all_dfs, ignore_index=True)
        combined = combined.drop_duplicates()

        logger.debug(f"      {data_type}/{app_id}: {len(all_dfs)} files, {len(combined):,} rows after dedup")
        
        # Transform and write separate files for each metric_date
        curated = self._transform_dataframe(data_type, combined, app_id, processing_date)
        
        if curated is None or curated.empty:
            return 0
        
        # Group by metric_date (dt column) and write separate parquet files
        total_rows = 0
        for metric_date_val, date_group in curated.groupby('dt'):
            output_key = f'appstore/curated/{data_type}/dt={metric_date_val}/app_id={app_id}/data.parquet'
            buffer = io.BytesIO()
            date_group.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            
            self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())
            total_rows += len(date_group)
        
        logger.debug(f"      Wrote {total_rows:,} rows across {curated['dt'].nunique()} metric dates")
        return total_rows

    def _curate_app_data_with_lookback(self, data_type: str, app_id: str, metric_date: str) -> int:
        """Curate data for a specific metric_date, aggregating from multiple processing dates
        
        Apple refines data over 3-4 days. For metric_date=Jan 5:
        - dt=2026-01-05 may have partial data
        - dt=2026-01-06 has more complete data  
        - dt=2026-01-07 has refined data
        - dt=2026-01-09 has final data
        
        Strategy: Collect data from all processing dates, keep the LATEST version for each unique key.
        """
        base_date = datetime.strptime(metric_date, '%Y-%m-%d')
        all_dfs = []
        
        # Check all processing dates where this metric_date's data might exist
        for day_offset in range(self.LOOKBACK_DAYS + 1):
            processing_date = (base_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')
            prefix = f'appstore/raw/{data_type}/dt={processing_date}/app_id={app_id}/'
            
            try:
                response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                
                for obj in response.get('Contents', []):
                    if not obj['Key'].endswith('.csv'):
                        continue
                    
                    filename = obj['Key'].split('/')[-1].lower()
                    
                    # Only use STANDARD files
                    if 'detailed' in filename or 'performance' in filename:
                        continue
                    
                    csv_response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                    content = csv_response['Body'].read().decode('utf-8')
                    df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
                    
                    if df.empty or 'Date' not in df.columns:
                        continue
                    
                    # Filter to only the metric_date we care about
                    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                    df = df[df['Date'] == metric_date]
                    
                    if not df.empty:
                        df['_processing_date'] = processing_date
                        all_dfs.append(df)
                        
            except Exception as e:
                logger.debug(f"Error reading from dt={processing_date}: {e}")
                continue
        
        if not all_dfs:
            return 0
        
        # Combine all data and deduplicate, keeping latest processing date
        combined = pd.concat(all_dfs, ignore_index=True)
        
        # Sort by processing_date descending to keep latest data
        combined = combined.sort_values('_processing_date', ascending=False)
        
        # Deduplicate keeping first (latest processing date)
        # Group by all columns except _processing_date
        dedup_cols = [c for c in combined.columns if c != '_processing_date']
        combined = combined.drop_duplicates(subset=dedup_cols, keep='first')
        
        # Get the latest processing date used (for audit trail)
        latest_processing_date = combined['_processing_date'].max()
        
        # Remove temp column and transform
        combined = combined.drop(columns=['_processing_date'])
        curated = self._transform_dataframe(data_type, combined, app_id, latest_processing_date)
        
        if curated is None or curated.empty:
            return 0
        
        # Write to S3 - curated path uses metric_date as partition
        output_key = f'appstore/curated/{data_type}/dt={metric_date}/app_id={app_id}/data.parquet'
        buffer = io.BytesIO()
        curated.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        
        self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())
        logger.debug(f"      Wrote {len(curated)} rows to dt={metric_date} (from processing_date={latest_processing_date})")
        
        return len(curated)

    def _curate_app_data(self, data_type: str, app_id: str, target_date: str) -> int:
        """Curate data for a specific app/type/date
        
        Key fixes:
        1. Uses STANDARD files only (complete data, not just attributed)
        2. Partitions curated data by actual metric_date (from data),
           not by processingDate (when Apple made data available)
        
        File mapping:
        - downloads: app_downloads_standard_*.csv
        - installs: app_store_installation_and_deletion_standard_*.csv
        - engagement: app_store_discovery_and_engagement_standard_*.csv
        - sessions: app_sessions_standard_*.csv
        - purchases: app_store_purchases_standard_*.csv
        """
        prefix = f'appstore/raw/{data_type}/dt={target_date}/app_id={app_id}/'
        
        dfs = []
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        
        for obj in response.get('Contents', []):
            if not obj['Key'].endswith('.csv'):
                continue
            
            filename = obj['Key'].split('/')[-1].lower()
            
            # Only use STANDARD files for complete data
            # Skip DETAILED files (only have attributed data, missing ~85%)
            if 'detailed' in filename:
                continue
            
            # Skip performance files (different metrics)
            if 'performance' in filename:
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
        
        # Group by metric_date and write separate parquet files for each date
        # This ensures dt partition matches the actual metric date, not processing date
        total_rows = 0
        for metric_date_val, date_group in curated.groupby('dt'):
            output_key = f'appstore/curated/{data_type}/dt={metric_date_val}/app_id={app_id}/data.parquet'
            buffer = io.BytesIO()
            date_group.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            
            self.s3.put_object(Bucket=self.bucket, Key=output_key, Body=buffer.getvalue())
            total_rows += len(date_group)
            logger.debug(f"      Wrote {len(date_group)} rows to dt={metric_date_val}")
        
        return total_rows
    
    def _transform_dataframe(self, data_type: str, df: pd.DataFrame, app_id: str, target_date: str) -> Optional[pd.DataFrame]:
        """Transform raw DataFrame to curated schema with proper filtering and deduplication
        
        Key fixes:
        1. Downloads: Filter to 'First-time download' + 'Redownload' only (matches Apple Analytics)
        2. Installs: Filter to 'Install' events only
        3. Engagement: Separate by Event type (Impression, Page view, Tap)
        4. Sets dt partition to actual metric_date (from data), not processingDate
        5. Adds processing_date column for audit trail
        """
        try:
            # First, deduplicate raw data (Apple sometimes provides overlapping segments)
            df = df.drop_duplicates()
            
            # Skip if no Date column
            if 'Date' not in df.columns:
                return None
            
            # Convert metric_date once - this is the actual date of the metrics
            metric_dates = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            
            if data_type == 'downloads':
                # Filter to only First-time download + Redownload (Apple Analytics definition)
                # Excludes: Auto-update, Manual update, Restore
                if 'Download Type' in df.columns:
                    df = df[df['Download Type'].isin(['First-time download', 'Redownload'])]
                    if df.empty:
                        return None
                    metric_dates = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                
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
                    'processing_date': target_date,
                    'dt': metric_dates
                })
                curated = curated[curated['total_downloads'] > 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'download_type', 
                              'source_type', 'device', 'platform_version', 'app_id_part', 'processing_date', 'dt']
                result = curated.groupby(group_cols, as_index=False).agg({'total_downloads': 'sum'})
                result['total_downloads'] = result['total_downloads'].astype('int64')
                result['app_id'] = result['app_id'].astype('int64')
                return result
                
            elif data_type == "engagement":
                # Use STANDARD files only (ETL already filters out detailed files).
                # Dedup strategy: plain drop_duplicates() removes exact row duplicates
                # from overlapping segment files, but keeps rows where same dimensions
                # have different Counts (these are different measurement windows that
                # Apple sums together). Validated: 48,059 vs Apple 49,615 = 3.1% match.
            
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'event_type': df.get('Event', 'Unknown'),
                    'page_type': df.get('Page Type', ''),
                    'source_type': df.get('Source Type', ''),
                    'device': df.get('Device', ''),
                    'platform_version': df.get('Platform Version', ''),
                    'impressions': pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64'),
                    'impressions_unique': pd.to_numeric(df.get('Unique Counts', 0), errors='coerce').fillna(0).astype('int64'),
                    'app_id_part': int(app_id),
                    'processing_date': target_date,
                    'dt': metric_dates
                })
                curated = curated[(curated['impressions'] > 0) | (curated['impressions_unique'] > 0)]
            
                # Aggregate by dimension columns, using SUM
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'event_type',
                              'page_type', 'source_type', 'device', 'platform_version',
                              'app_id_part', 'processing_date', 'dt']
                result = curated.groupby(group_cols, as_index=False).agg({
                    'impressions': 'sum',
                    'impressions_unique': 'sum'
                })
                result['impressions'] = result['impressions'].astype('int64')
                result['impressions_unique'] = result['impressions_unique'].astype('int64')
                result['app_id'] = result['app_id'].astype('int64')
                return result


                
            elif data_type == 'sessions':
                # Sessions use 'Sessions' column, not 'Counts'
                # FIX (2026-05-21): Apple ships each logical row across multiple segment
                # files with conflicting counts (preliminary vs refined). Carry raw dims
                # into a temp DataFrame, dedup on full dim tuple keeping max sessions,
                # then aggregate down to the storage schema. Without this, sessions and
                # unique_devices over-count by 2-9x.
                sessions_col = 'Sessions' if 'Sessions' in df.columns else 'Counts'
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'sessions': pd.to_numeric(df[sessions_col], errors='coerce').fillna(0).astype('int64'),
                    'total_duration': pd.to_numeric(df.get('Total Session Duration', 0), errors='coerce').fillna(0).astype('int64'),
                    'unique_devices': pd.to_numeric(df.get('Unique Devices', 0), errors='coerce').fillna(0).astype('int64'),
                    'device': df.get('Device', ''),
                    'source_type': df.get('Source Type', ''),
                    'app_version': df.get('App Version', ''),
                    'platform_version': df.get('Platform Version', ''),
                    'app_id_part': int(app_id),
                    'processing_date': target_date,
                    'dt': metric_dates
                })
                curated = curated[curated['sessions'] > 0]
                # Dedup on full raw dim tuple, keep row with max sessions count
                full_dim_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'device',
                                 'source_type', 'app_version', 'platform_version',
                                 'app_id_part', 'processing_date', 'dt']
                curated = curated.sort_values('sessions', ascending=False).drop_duplicates(subset=full_dim_cols, keep='first')
                # Aggregate to storage schema (curated_sessions has no source_type/version columns)
                storage_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'device',
                                'app_id_part', 'processing_date', 'dt']
                result = curated.groupby(storage_cols, as_index=False).agg({
                    'sessions': 'sum',
                    'total_duration': 'sum',
                    'unique_devices': 'sum'
                })
                for col in ["sessions", "total_duration", "unique_devices", "app_id"]:
                    if col in result.columns:
                        result[col] = result[col].astype("int64")
                return result
                
            elif data_type == 'installs':
                # Filter to 'Install' events only (exclude 'Delete')
                if 'Event' in df.columns:
                    df = df[df['Event'] == 'Install']
                    if df.empty:
                        return None
                    metric_dates = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                
                # FIX (2026-05-21): Same segment-overlap dedup pattern as sessions.
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'installs': pd.to_numeric(df['Counts'], errors='coerce').fillna(0).astype('int64'),
                    'unique_devices': pd.to_numeric(df.get('Unique Devices', 0), errors='coerce').fillna(0).astype('int64'),
                    'download_type': df.get('Download Type', ''),
                    'device': df.get('Device', ''),
                    'source_type': df.get('Source Type', ''),
                    'platform_version': df.get('Platform Version', ''),
                    'app_id_part': int(app_id),
                    'processing_date': target_date,
                    'dt': metric_dates
                })
                curated = curated[curated['installs'] > 0]
                full_dim_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'download_type',
                                 'device', 'source_type', 'platform_version',
                                 'app_id_part', 'processing_date', 'dt']
                curated = curated.sort_values('installs', ascending=False).drop_duplicates(subset=full_dim_cols, keep='first')
                storage_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'download_type',
                                'device', 'app_id_part', 'processing_date', 'dt']
                result = curated.groupby(storage_cols, as_index=False).agg({
                    'installs': 'sum',
                    'unique_devices': 'sum'
                })
                for col in ['installs', 'unique_devices', 'app_id']:
                    if col in result.columns:
                        result[col] = result[col].astype('int64')
                return result
                
            elif data_type == 'purchases':
                curated = pd.DataFrame({
                    'metric_date': pd.to_datetime(df['Date']).dt.date,
                    'app_name': df['App Name'],
                    'app_id': pd.to_numeric(df['App Apple Identifier'], errors='coerce').fillna(0).astype('int64'),
                    'territory': df['Territory'],
                    'purchases': pd.to_numeric(df.get('Purchases', df.get('Counts', 0)), errors='coerce').fillna(0).astype('int64'),
                    'proceeds_usd': pd.to_numeric(df.get('Proceeds in USD', 0), errors='coerce').fillna(0.0),
                    'paying_users': pd.to_numeric(df.get('Paying Users', 0), errors='coerce').fillna(0).astype('int64'),
                    'device': df.get('Device', ''),
                    'app_id_part': int(app_id),
                    'processing_date': target_date,
                    'dt': metric_dates
                })
                # Include refunds (negative purchases) to match Apple Analytics
                # Filter: purchases != 0 (not > 0) to include refunds
                curated = curated[curated['purchases'] != 0]
                group_cols = ['metric_date', 'app_name', 'app_id', 'territory', 'device', 'app_id_part', 'processing_date', 'dt']
                result = curated.groupby(group_cols, as_index=False).agg({
                    'purchases': 'sum', 
                    'proceeds_usd': 'sum',
                    'paying_users': 'sum'
                })
                for col in ["purchases", "paying_users", "app_id"]:
                    if col in result.columns:
                        result[col] = result[col].astype("int64")
                return result
            
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
            except Exception as e:
                result['errors'].append(f"{table}: {str(e)}")
        
        logger.info(f"   ✅ Refreshed {result['tables_refreshed']} tables")
        return result
    
    # =========================================================================
    # MAIN RUN METHOD
    # =========================================================================
    
    def run(self, target_date: Optional[str] = None, app_id: Optional[str] = None,
            backfill_days: int = 0) -> Dict:
        """Run the complete ETL pipeline"""
        print("=" * 80)
        print("🍎 APPLE ANALYTICS - UNIFIED ETL PIPELINE")
        print("=" * 80)
        
        # Determine dates
        if target_date:
            base_date = datetime.strptime(target_date, '%Y-%m-%d')
        else:
            base_date = datetime.now(timezone.utc) - timedelta(days=1)
        
        if backfill_days > 0:
            dates = [(base_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(backfill_days)]
        else:
            dates = [base_date.strftime('%Y-%m-%d')]
        
        app_ids = self.get_app_ids(app_id)
        
        print(f"📅 Dates: {len(dates)} ({dates[-1]} to {dates[0]})")
        print(f"📱 Apps: {len(app_ids)}")
        print("=" * 80)
        
        # Process each date
        for date_str in dates:
            print(f"\n{'='*60}")
            print(f"📆 Processing: {date_str}")
            print(f"{'='*60}")
            
            # EXTRACT
            print("\n🔄 PHASE 1: EXTRACT")
            for idx, aid in enumerate(app_ids):
                # Add delay between apps to avoid rate limiting (except for first app)
                if idx > 0:
                    logger.info(f"⏱️  Waiting 10 seconds before processing next app...")
                    time.sleep(10)
                
                result = self.extract_app_data(aid, date_str)
                self.results['apps_processed'] += 1
                if result['success']:
                    self.results['apps_successful'] += 1
                    self.results['files_extracted'] += result['files']
            
            # EXTRACT REVIEWS (App Store Connect Customer Reviews API)
            try:
                review_result = self.extract_all_reviews(date_str, specific_app_id=app_id)
                self.results['reviews_extracted'] += review_result.get('new_reviews', 0)
                self.results['reviews_apps'] += review_result.get('apps', 0)
                self.results['reviews_apps'] += review_result.get('apps', 0)
                logger.info(f"   Reviews: {review_result.get('new_reviews', 0)} new from {review_result.get('apps', 0)} apps")
            except Exception as e:
                logger.warning(f"   Review extraction skipped: {e}")

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
        print(f"Reviews:         {self.results['reviews_extracted']} new from {self.results['reviews_apps']} apps")
        print("=" * 80)
    
    def _save_results(self):
        self.results['end_time'] = datetime.now(timezone.utc).isoformat()
        results_file = os.path.join(SCRIPT_DIR, 'logs', f'unified_etl_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"Results saved to: {results_file}")


def main():
    parser = argparse.ArgumentParser(description='Apple Analytics Unified ETL')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--app-id', type=str, help='Specific app ID')
    parser.add_argument('--backfill', action='store_true', help='Backfill mode')
    parser.add_argument('--days', type=int, default=30, help='Days to backfill')
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
            backfill_days=args.days if args.backfill else 0
        )


if __name__ == '__main__':
    main()
