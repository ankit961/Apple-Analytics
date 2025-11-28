#!/usr/bin/env python3
"""
Batch Curator - Process all raw data to curated Parquet
Aligns with the fixed Athena table schemas

Usage:
    python3 batch_curate.py                    # Process yesterday's data
    python3 batch_curate.py --date 2025-11-27  # Process specific date
    python3 batch_curate.py --all              # Process all available dates
"""

import boto3
import pandas as pd
import io
import argparse
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BatchCurator:
    """Batch process raw CSV data to curated Parquet format"""
    
    def __init__(self):
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.bucket = 'skidos-apptrack'
        
    def list_available_dates(self, data_type: str = 'downloads') -> List[str]:
        """List all dates with raw data"""
        prefix = f'appstore/raw/{data_type}/'
        dates = set()
        
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                path = common_prefix['Prefix']
                if 'dt=' in path:
                    date_part = path.split('dt=')[1].rstrip('/')
                    dates.add(date_part)
        
        return sorted(dates)
    
    def list_apps_for_date(self, data_type: str, date_str: str) -> List[str]:
        """List all apps with data for a specific date"""
        prefix = f'appstore/raw/{data_type}/dt={date_str}/'
        apps = []
        
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        for common_prefix in response.get('CommonPrefixes', []):
            path = common_prefix['Prefix']
            if 'app_id=' in path:
                app_id = path.split('app_id=')[1].rstrip('/')
                apps.append(app_id)
        
        return apps
    
    def read_raw_csv(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Read raw CSV from S3"""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(content), sep='\t', low_memory=False)
            return df
        except Exception as e:
            logger.warning(f"Failed to read {s3_key}: {e}")
            return None
    
    def _get_raw_files(self, data_type: str, app_id: str, date_str: str) -> List[pd.DataFrame]:
        """Get all raw CSV files for a data type/app/date"""
        prefix = f'appstore/raw/{data_type}/dt={date_str}/app_id={app_id}/'
        dfs = []
        
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' not in response:
                return dfs
            
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    df = self.read_raw_csv(obj['Key'])
                    if df is not None and not df.empty:
                        dfs.append(df)
        except Exception as e:
            logger.warning(f"Error listing {prefix}: {e}")
        
        return dfs
    
    def _save_parquet(self, df: pd.DataFrame, data_type: str, app_id: str, date_str: str) -> bool:
        """Save DataFrame as Parquet to S3"""
        try:
            output_key = f'appstore/curated/{data_type}/dt={date_str}/app_id={app_id}/data.parquet'
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=output_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
            return True
        except Exception as e:
            logger.error(f"Failed to save parquet: {e}")
            return False

    def process_downloads(self, app_id: str, date_str: str) -> Dict:
        """Process downloads data for an app/date"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            dfs = self._get_raw_files('downloads', app_id, date_str)
            if not dfs:
                return result
            
            combined = pd.concat(dfs, ignore_index=True)
            
            # Transform to curated schema
            curated = pd.DataFrame()
            curated['metric_date'] = pd.to_datetime(combined['Date']).dt.date
            curated['app_name'] = combined['App Name']
            curated['app_id'] = pd.to_numeric(combined['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
            curated['territory'] = combined['Territory']
            curated['total_downloads'] = pd.to_numeric(combined['Counts'], errors='coerce').fillna(0).astype('int64')
            curated['download_type'] = combined.get('Download Type', '')
            curated['source_type'] = combined.get('Source Type', '')
            curated['device'] = combined.get('Device', '')
            curated['platform_version'] = combined.get('Platform Version', '')
            curated['page_type'] = combined.get('Page Type', '')
            curated['app_id_part'] = int(app_id)
            curated['dt'] = date_str
            
            curated = curated[curated['total_downloads'] > 0].drop_duplicates()
            
            if curated.empty:
                return result
            
            if self._save_parquet(curated, 'downloads', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Downloads {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Downloads {app_id}/{date_str}: {e}")
        
        return result
    
    def process_engagement(self, app_id: str, date_str: str) -> Dict:
        """Process engagement data for an app/date"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            dfs = self._get_raw_files('engagement', app_id, date_str)
            if not dfs:
                return result
            
            combined = pd.concat(dfs, ignore_index=True)
            
            curated = pd.DataFrame()
            curated['metric_date'] = pd.to_datetime(combined['Date']).dt.date
            curated['app_name'] = combined['App Name']
            curated['app_id'] = pd.to_numeric(combined['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
            curated['source_type'] = combined.get('Source Type', '')
            curated['page_type'] = combined.get('Page Type', '')
            curated['territory'] = combined['Territory']
            curated['event_type'] = combined.get('Event', '')
            curated['engagement_type'] = combined.get('Engagement Type', '')
            curated['impressions'] = pd.to_numeric(combined['Counts'], errors='coerce').fillna(0).astype('int64')
            curated['impressions_unique'] = pd.to_numeric(combined.get('Unique Counts', 0), errors='coerce').fillna(0).astype('int64')
            curated['device'] = combined.get('Device', '')
            curated['platform_version'] = combined.get('Platform Version', '')
            curated['app_id_part'] = int(app_id)
            curated['dt'] = date_str
            
            curated = curated[(curated['impressions'] > 0) | (curated['impressions_unique'] > 0)].drop_duplicates()
            
            if curated.empty:
                return result
            
            if self._save_parquet(curated, 'engagement', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Engagement {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Engagement {app_id}/{date_str}: {e}")
        
        return result
    
    def process_sessions(self, app_id: str, date_str: str) -> Dict:
        """Process sessions data for an app/date"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            dfs = self._get_raw_files('sessions', app_id, date_str)
            if not dfs:
                return result
            
            combined = pd.concat(dfs, ignore_index=True)
            
            curated = pd.DataFrame()
            curated['metric_date'] = pd.to_datetime(combined['Date']).dt.date
            curated['app_name'] = combined['App Name']
            curated['app_id'] = pd.to_numeric(combined['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
            curated['app_version'] = combined.get('App Version', '')
            curated['device'] = combined.get('Device', '')
            curated['platform_version'] = combined.get('Platform Version', '')
            curated['source_type'] = combined.get('Source Type', '')
            curated['territory'] = combined['Territory']
            curated['sessions'] = pd.to_numeric(combined['Sessions'], errors='coerce').fillna(0).astype('int64')
            curated['total_session_duration'] = pd.to_numeric(combined['Total Session Duration'], errors='coerce').fillna(0).astype('int64')
            curated['unique_devices'] = pd.to_numeric(combined['Unique Devices'], errors='coerce').fillna(0).astype('int64')
            curated['app_id_part'] = int(app_id)
            curated['dt'] = date_str
            
            curated = curated[curated['sessions'] > 0].drop_duplicates()
            
            if curated.empty:
                return result
            
            if self._save_parquet(curated, 'sessions', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Sessions {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Sessions {app_id}/{date_str}: {e}")
        
        return result
    
    def process_installs(self, app_id: str, date_str: str) -> Dict:
        """Process installs data for an app/date"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            dfs = self._get_raw_files('installs', app_id, date_str)
            if not dfs:
                return result
            
            combined = pd.concat(dfs, ignore_index=True)
            
            curated = pd.DataFrame()
            curated['metric_date'] = pd.to_datetime(combined['Date']).dt.date
            curated['app_name'] = combined['App Name']
            curated['app_id'] = pd.to_numeric(combined['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
            curated['event'] = combined.get('Event', '')
            curated['download_type'] = combined.get('Download Type', '')
            curated['app_version'] = combined.get('App Version', '')
            curated['device'] = combined.get('Device', '')
            curated['platform_version'] = combined.get('Platform Version', '')
            curated['source_type'] = combined.get('Source Type', '')
            curated['territory'] = combined['Territory']
            curated['counts'] = pd.to_numeric(combined['Counts'], errors='coerce').fillna(0).astype('int64')
            curated['unique_devices'] = pd.to_numeric(combined['Unique Devices'], errors='coerce').fillna(0).astype('int64')
            curated['app_id_part'] = int(app_id)
            curated['dt'] = date_str
            
            curated = curated[curated['counts'] > 0].drop_duplicates()
            
            if curated.empty:
                return result
            
            if self._save_parquet(curated, 'installs', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Installs {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Installs {app_id}/{date_str}: {e}")
        
        return result
    
    def process_purchases(self, app_id: str, date_str: str) -> Dict:
        """Process purchases data for an app/date"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            dfs = self._get_raw_files('purchases', app_id, date_str)
            if not dfs:
                return result
            
            combined = pd.concat(dfs, ignore_index=True)
            
            curated = pd.DataFrame()
            curated['metric_date'] = pd.to_datetime(combined['Date']).dt.date
            curated['app_name'] = combined['App Name']
            curated['app_id'] = pd.to_numeric(combined['App Apple Identifier'], errors='coerce').fillna(0).astype('int64')
            curated['purchase_type'] = combined.get('Purchase Type', '')
            curated['content_name'] = combined.get('Content Name', '')
            curated['payment_method'] = combined.get('Payment Method', '')
            curated['device'] = combined.get('Device', '')
            curated['platform_version'] = combined.get('Platform Version', '')
            curated['source_type'] = combined.get('Source Type', '')
            curated['territory'] = combined['Territory']
            curated['purchases'] = pd.to_numeric(combined['Purchases'], errors='coerce').fillna(0).astype('int64')
            curated['proceeds_usd'] = pd.to_numeric(combined.get('Proceeds in USD', 0), errors='coerce').fillna(0.0)
            curated['sales_usd'] = pd.to_numeric(combined.get('Sales in USD', 0), errors='coerce').fillna(0.0)
            curated['paying_users'] = pd.to_numeric(combined.get('Paying Users', 0), errors='coerce').fillna(0).astype('int64')
            curated['app_id_part'] = int(app_id)
            curated['dt'] = date_str
            
            curated = curated[curated['purchases'] > 0].drop_duplicates()
            
            if curated.empty:
                return result
            
            if self._save_parquet(curated, 'purchases', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Purchases {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Purchases {app_id}/{date_str}: {e}")
        
        return result
    
    def process_reviews(self, app_id: str, date_str: str) -> Dict:
        """Process reviews data for an app/date (JSON format)"""
        result = {'app_id': app_id, 'date': date_str, 'rows': 0, 'success': False}
        
        try:
            prefix = f'appstore/raw/reviews/dt={date_str}/app_id={app_id}/'
            
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if 'Contents' not in response:
                return result
            
            all_reviews = []
            
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    try:
                        json_response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                        json_content = json_response['Body'].read().decode('utf-8')
                        data = json.loads(json_content)
                        
                        # Handle different JSON structures
                        if isinstance(data, list):
                            reviews = data
                        elif isinstance(data, dict):
                            reviews = data.get('data', data.get('reviews', [data]))
                        else:
                            continue
                        
                        for review in reviews:
                            if isinstance(review, dict):
                                all_reviews.append(review)
                    except Exception as e:
                        logger.warning(f"Failed to parse JSON {obj['Key']}: {e}")
            
            if not all_reviews:
                return result
            
            # Transform to curated schema
            curated_rows = []
            for review in all_reviews:
                # Extract attributes (Apple's format has nested structure)
                attrs = review.get('attributes', review)
                
                curated_rows.append({
                    'review_id': review.get('id', ''),
                    'app_id': int(app_id),
                    'review_date': attrs.get('createdDate', attrs.get('date', '')),
                    'rating': int(attrs.get('rating', 0)),
                    'title': attrs.get('title', ''),
                    'body': attrs.get('body', attrs.get('review', '')),
                    'reviewer_nickname': attrs.get('reviewerNickname', attrs.get('nickname', '')),
                    'territory': attrs.get('territory', attrs.get('storefront', '')),
                    'app_id_part': int(app_id),
                    'dt': date_str
                })
            
            curated = pd.DataFrame(curated_rows)
            
            if curated.empty:
                return result
            
            # Deduplicate by review_id
            curated = curated.drop_duplicates(subset=['review_id'])
            
            if self._save_parquet(curated, 'reviews', app_id, date_str):
                result['rows'] = len(curated)
                result['success'] = True
                logger.info(f"✅ Reviews {app_id}/{date_str}: {len(curated)} rows")
                
        except Exception as e:
            logger.error(f"❌ Reviews {app_id}/{date_str}: {e}")
        
        return result
    
    def run(self, date_str: Optional[str] = None, process_all: bool = False):
        """Run batch curation for all data types"""
        print("=" * 60)
        print("BATCH CURATOR - Raw CSV to Curated Parquet")
        print("=" * 60)
        
        if process_all:
            dates = self.list_available_dates('downloads')
            print(f"📅 Processing all {len(dates)} dates")
        elif date_str:
            dates = [date_str]
            print(f"📅 Processing date: {date_str}")
        else:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            dates = [yesterday]
            print(f"📅 Processing yesterday: {yesterday}")
        
        # Data types to process
        data_types = [
            ('downloads', self.process_downloads),
            ('engagement', self.process_engagement),
            ('sessions', self.process_sessions),
            ('installs', self.process_installs),
            ('purchases', self.process_purchases),
            ('reviews', self.process_reviews),
        ]
        
        total_results = {dt: {'apps': 0, 'rows': 0, 'errors': 0} for dt, _ in data_types}
        
        for dt in dates:
            print(f"\n📆 Processing {dt}...")
            
            for data_type, processor in data_types:
                apps = self.list_apps_for_date(data_type, dt)
                if apps:
                    print(f"   {data_type.capitalize()}: {len(apps)} apps")
                    
                    for app_id in apps:
                        result = processor(app_id, dt)
                        if result['success']:
                            total_results[data_type]['apps'] += 1
                            total_results[data_type]['rows'] += result['rows']
                        else:
                            total_results[data_type]['errors'] += 1
        
        # Summary
        print("\n" + "=" * 60)
        print("CURATION SUMMARY")
        print("=" * 60)
        for data_type, stats in total_results.items():
            print(f"{data_type.capitalize():12} {stats['apps']:3} apps, {stats['rows']:>12,} rows, {stats['errors']} errors")
        
        return total_results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch curate raw data to Parquet')
    parser.add_argument('--date', type=str, help='Process specific date (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Process all available dates')
    
    args = parser.parse_args()
    
    curator = BatchCurator()
    curator.run(date_str=args.date, process_all=args.all)
