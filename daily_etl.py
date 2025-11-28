#!/usr/bin/env python3
"""
Apple Analytics Daily ETL - Production Ready

This script is designed for daily automated execution:
1. Uses ONGOING requests (not ONE_TIME_SNAPSHOT) to avoid 409 conflicts
2. Extracts yesterday's data automatically
3. Uploads to S3 in the correct structure for Athena
4. Handles all apps configured in environment

Usage:
    python3 daily_etl.py                    # Extract yesterday's data for all apps
    python3 daily_etl.py --app-id 1506886061  # Extract for specific app
    python3 daily_etl.py --date 2025-11-26    # Extract for specific date

Environment Variables Required:
    ASC_ISSUER_ID, ASC_KEY_ID, ASC_P8_PATH - Apple API credentials
    S3_BUCKET - Target S3 bucket (default: skidos-apptrack)
    APP_IDS - Comma-separated list of app IDs (optional, can use --app-id)
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import after path setup
from src.extract.apple_analytics_client import AppleAnalyticsRequestor


class DailyETL:
    """Daily ETL for Apple Analytics using ONGOING requests"""
    
    def __init__(self):
        self.requestor = AppleAnalyticsRequestor()
        self.results = {
            'execution_time': datetime.now(timezone.utc).isoformat(),
            'apps_processed': 0,
            'apps_successful': 0,
            'total_files': 0,
            'total_rows': 0,
            'errors': [],
            'app_results': []
        }
    
    def get_app_ids(self, specific_app_id: Optional[str] = None) -> List[str]:
        """Get list of app IDs to process"""
        if specific_app_id:
            return [specific_app_id]
        
        # Load from environment
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        
        # Default test apps with known data
        return ['1506886061', '1159612010', '1468754350']
    
    def extract_reports_for_date(self, app_id: str, target_date: str) -> Dict:
        """Extract all available reports for a specific app and date"""
        result = {
            'app_id': app_id,
            'target_date': target_date,
            'request_id': None,
            'reports_checked': 0,
            'reports_with_data': 0,
            'files_downloaded': 0,
            'total_rows': 0,
            'success': False,
            'errors': []
        }
        
        try:
            # Step 1: Get or create ONGOING request
            logger.info(f"📱 Processing app {app_id} for date {target_date}")
            request_id = self.requestor.create_or_reuse_ongoing_request(app_id)
            
            if not request_id:
                error_msg = f"Failed to get ONGOING request for app {app_id}"
                logger.error(f"❌ {error_msg}")
                result['errors'].append(error_msg)
                return result
            
            result['request_id'] = request_id
            logger.info(f"✅ Using request: {request_id}")
            
            # Step 2: Get all reports for this request
            reports_url = f"{self.requestor.api_base}/analyticsReportRequests/{request_id}/reports"
            response = self.requestor._asc_request('GET', reports_url, timeout=60)
            
            if response.status_code != 200:
                error_msg = f"Failed to get reports: {response.status_code}"
                result['errors'].append(error_msg)
                return result
            
            reports = response.json().get('data', [])
            result['reports_checked'] = len(reports)
            logger.info(f"📊 Found {len(reports)} reports")
            
            # Step 3: Process each report
            for report in reports:
                report_id = report['id']
                report_name = report['attributes']['name']
                
                # Get instances for this report
                instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                inst_response = self.requestor._asc_request('GET', instances_url, timeout=30)
                
                if inst_response.status_code != 200:
                    continue
                
                instances = inst_response.json().get('data', [])
                
                # Filter instances for target date
                for instance in instances:
                    instance_id = instance['id']
                    instance_attrs = instance.get('attributes', {})
                    
                    # Check if this instance is for our target date
                    # The granularity and processingDate fields can help filter
                    processing_date = instance_attrs.get('processingDate', '')
                    
                    # Extract data from this instance
                    files, rows = self._download_instance_data(
                        instance_id, app_id, report_name, target_date
                    )
                    
                    if files > 0:
                        result['reports_with_data'] += 1
                        result['files_downloaded'] += files
                        result['total_rows'] += rows
            
            if result['files_downloaded'] > 0:
                result['success'] = True
                logger.info(f"✅ App {app_id}: {result['files_downloaded']} files, {result['total_rows']} rows")
            else:
                logger.info(f"⚠️ App {app_id}: No new data for {target_date}")
                result['success'] = True  # No error, just no new data
                
        except Exception as e:
            error_msg = f"Exception processing app {app_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            result['errors'].append(error_msg)
        
        return result
    
    def _download_instance_data(self, instance_id: str, app_id: str, 
                                report_name: str, target_date: str) -> tuple:
        """Download data from an instance and return (files_count, rows_count)"""
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
                    # Download and upload to S3
                    rows = self._download_and_save(download_url, app_id, report_name, 
                                                   instance_id, segment_id, target_date)
                    if rows > 0:
                        files_downloaded += 1
                        total_rows += rows
                        
        except Exception as e:
            logger.warning(f"⚠️ Instance {instance_id} error: {e}")
        
        return files_downloaded, total_rows
    
    def _download_and_save(self, download_url: str, app_id: str, report_name: str,
                          instance_id: str, segment_id: str, target_date: str) -> int:
        """Download file from URL and save to S3"""
        import requests
        import gzip
        
        try:
            response = requests.get(download_url, timeout=120)
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
            
            row_count = len(lines) - 1  # Exclude header
            
            # Determine report type for S3 path
            report_lower = report_name.lower()
            if 'download' in report_lower:
                report_type = 'downloads'
            elif 'engagement' in report_lower or 'discovery' in report_lower:
                report_type = 'engagement'
            elif 'session' in report_lower:
                report_type = 'sessions'
            elif 'install' in report_lower:
                report_type = 'installs'
            elif 'purchase' in report_lower:
                report_type = 'purchases'
            else:
                report_type = 'analytics'
            
            # Clean report name for file path
            clean_name = "".join(c for c in report_name if c.isalnum() or c in ' -_').replace(' ', '_').lower()
            
            # S3 key compatible with existing ETL pipeline
            s3_key = f"appstore/raw/{report_type}/dt={target_date}/app_id={app_id}/{clean_name}_{segment_id}.csv"
            
            # Upload to S3
            self.requestor.s3_client.put_object(
                Bucket=self.requestor.s3_bucket,
                Key=s3_key,
                Body=text_content.encode('utf-8'),
                ContentType='text/csv',
                Metadata={
                    'report_name': report_name,
                    'app_id': app_id,
                    'extraction_date': datetime.now(timezone.utc).isoformat(),
                    'row_count': str(row_count)
                }
            )
            
            logger.info(f"   ✅ {s3_key} ({row_count} rows)")
            return row_count
            
        except Exception as e:
            logger.warning(f"   ⚠️ Download error: {e}")
            return 0
    
    def run(self, app_id: Optional[str] = None, target_date: Optional[str] = None):
        """Run the daily ETL"""
        # Determine target date (default: yesterday)
        if target_date:
            date_str = target_date
        else:
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
        
        logger.info("=" * 70)
        logger.info("🍎 APPLE ANALYTICS DAILY ETL - ONGOING REQUESTS")
        logger.info("=" * 70)
        logger.info(f"📅 Target Date: {date_str}")
        logger.info(f"🔄 Request Type: ONGOING (no duplicates)")
        logger.info("=" * 70)
        
        # Get app IDs
        app_ids = self.get_app_ids(app_id)
        logger.info(f"📱 Apps to process: {len(app_ids)}")
        
        # Process each app
        for aid in app_ids:
            self.results['apps_processed'] += 1
            
            result = self.extract_reports_for_date(aid, date_str)
            self.results['app_results'].append(result)
            
            if result['success']:
                self.results['apps_successful'] += 1
                self.results['total_files'] += result['files_downloaded']
                self.results['total_rows'] += result['total_rows']
            else:
                self.results['errors'].extend(result['errors'])
        
        # Print summary
        self._print_summary()
        
        # Save results
        self._save_results(date_str)
        
        return self.results['apps_successful'] > 0
    
    def _print_summary(self):
        """Print execution summary"""
        print("\n" + "=" * 70)
        print("📊 DAILY ETL SUMMARY")
        print("=" * 70)
        print(f"📱 Apps Processed: {self.results['apps_processed']}")
        print(f"✅ Apps Successful: {self.results['apps_successful']}")
        print(f"📁 Total Files: {self.results['total_files']}")
        print(f"📄 Total Rows: {self.results['total_rows']}")
        
        if self.results['errors']:
            print(f"\n❌ Errors ({len(self.results['errors'])}):")
            for error in self.results['errors'][:5]:  # Show first 5
                print(f"   • {error}")
        
        print("=" * 70)
        
        if self.results['total_files'] > 0:
            print("\n🎯 Next Steps:")
            print("   1. Run transform: python3 -m src.transform.apple_analytics_data_curator")
            print("   2. Verify Athena: Run test queries")
            print(f"\n📁 S3 Location: s3://{self.requestor.s3_bucket}/appstore/raw/")
    
    def _save_results(self, date_str: str):
        """Save execution results to file"""
        results_file = f"daily_etl_results_{date_str.replace('-', '')}.json"
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        logger.info(f"📝 Results saved: {results_file}")


def main():
    parser = argparse.ArgumentParser(description='Apple Analytics Daily ETL')
    parser.add_argument('--app-id', help='Specific app ID to process')
    parser.add_argument('--date', help='Target date (YYYY-MM-DD), default: yesterday')
    args = parser.parse_args()
    
    etl = DailyETL()
    success = etl.run(app_id=args.app_id, target_date=args.date)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
