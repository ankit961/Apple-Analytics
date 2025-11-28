#!/usr/bin/env python3
"""
Focused Apple Analytics Data Extractor

Extract only the business-critical reports that actually have data instances.
Focus on downloads, installs, sessions, purchases, and engagement metrics.
"""

import sys
import json
import time
import logging
import os
import gzip
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import boto3
import requests

# Import the Apple Analytics client
from .apple_analytics_client import AppleAnalyticsRequestor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FocusedAppleDataExtractor:
    """Extract Apple Analytics data focusing on reports with actual instances"""
    
    def __init__(self):
        """Initialize the extractor"""
        self.requestor = AppleAnalyticsRequestor()
        self.s3_client = boto3.client('s3')
        self.s3_bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        
        # Focus on key business metrics
        self.target_report_names = [
            'App Downloads Standard',
            'App Downloads Detailed', 
            'App Store Installation and Deletion Standard',
            'App Store Installation and Deletion Detailed',
            'App Sessions Standard',
            'App Sessions Detailed',
            'App Store Purchases Standard',
            'App Store Purchases Detailed',
            'App Store Discovery and Engagement Standard',
            'App Store Discovery and Engagement Detailed',
            'App Install Performance'
        ]
        
        self.extraction_stats = {
            'total_reports_checked': 0,
            'reports_with_data': 0,
            'total_instances': 0,
            'total_segments': 0,
            'files_downloaded': 0,
            'total_rows': 0,
            'errors': []
        }
    
    def extract_instance_data(self, instance_id: str, instance_attrs: Dict, 
                            report_name: str, app_id: str) -> Dict:
        """Extract data from a specific analytics instance"""
        result = {
            'instance_id': instance_id,
            'files_downloaded': 0,
            'total_rows': 0,
            'success': False,
            'errors': []
        }
        
        try:
            # Get segments for this instance
            segments_url = f"{self.requestor.api_base}/analyticsReportInstances/{instance_id}/segments"
            seg_response = self.requestor._asc_request('GET', segments_url, timeout=30)
            
            if seg_response.status_code == 200:
                segments_data = seg_response.json()
                segments = segments_data.get('data', [])
                
                logger.info(f"   📊 Instance {instance_id}: {len(segments)} segments")
                
                for segment in segments:
                    segment_id = segment['id']
                    
                    # Get download URL for this segment
                    download_url_endpoint = f"{self.requestor.api_base}/analyticsReportSegments/{segment_id}"
                    download_response = self.requestor._asc_request('GET', download_url_endpoint, timeout=30)
                    
                    if download_response.status_code == 200:
                        download_data = download_response.json()
                        download_attrs = download_data['data']['attributes']
                        
                        if 'url' in download_attrs:
                            download_url = download_attrs['url']
                            
                            # Download and save the CSV file
                            file_result = self.download_and_save_csv(
                                download_url, app_id, report_name, 
                                instance_id, segment_id, instance_attrs
                            )
                            
                            if file_result['success']:
                                result['files_downloaded'] += 1
                                result['total_rows'] += file_result['rows']
                                self.extraction_stats['files_downloaded'] += 1
                                self.extraction_stats['total_rows'] += file_result['rows']
                        else:
                            logger.warning(f"   ⚠️ No download URL in segment {segment_id}")
                    else:
                        error_msg = f"Segment download failed: {download_response.status_code}"
                        result['errors'].append(error_msg)
                        logger.warning(f"   ❌ {error_msg}")
                
                if result['files_downloaded'] > 0:
                    result['success'] = True
                    
            else:
                error_msg = f"Segments request failed: {seg_response.status_code}"
                result['errors'].append(error_msg)
                
        except Exception as e:
            error_msg = f"Instance extraction failed: {str(e)}"
            result['errors'].append(error_msg)
            logger.error(f"   ❌ {error_msg}")
        
        return result
    
    def download_and_save_csv(self, download_url: str, app_id: str, report_name: str,
                            instance_id: str, segment_id: str, instance_attrs: Dict) -> Dict:
        """Download CSV file and save to S3"""
        result = {'success': False, 'rows': 0}
        
        try:
            logger.info(f"   📥 Downloading: {report_name} - {segment_id}")
            
            # Download the file
            response = requests.get(download_url, timeout=60)
            response.raise_for_status()
            
            content = response.content
            
            # Handle gzip compression if present
            if content[:2] == b'\x1f\x8b':  # gzip magic number
                try:
                    content = gzip.decompress(content)
                    logger.info(f"   🗜️ Decompressed gzip data ({len(content)} bytes)")
                except Exception as gz_error:
                    logger.error(f"   ❌ Gzip decompression failed: {gz_error}")
                    return result
            
            # Process CSV content
            try:
                text_content = content.decode('utf-8')
            except UnicodeDecodeError as decode_error:
                # Try with different encoding
                try:
                    text_content = content.decode('latin-1')
                    logger.warning(f"   ⚠️ Used latin-1 encoding fallback")
                except Exception as final_error:
                    logger.error(f"   ❌ Decode failed: {final_error}")
                    return result
            lines = text_content.strip().split('\n')
            
            if len(lines) > 1:  # Has header + data
                result['rows'] = len(lines) - 1  # Subtract header
                
                # Create S3 key with proper structure
                date_str = datetime.now().strftime('%Y-%m-%d')
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # Clean report name for file path
                clean_report_name = "".join(c for c in report_name if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_').lower()
                
                # Determine report type for ETL compatibility
                if 'downloads' in clean_report_name:
                    report_type = 'downloads'
                elif 'installation' in clean_report_name or 'deletion' in clean_report_name:
                    report_type = 'installs'
                elif 'sessions' in clean_report_name:
                    report_type = 'sessions'
                elif 'purchases' in clean_report_name:
                    report_type = 'purchases'
                elif 'discovery' in clean_report_name or 'engagement' in clean_report_name:
                    report_type = 'engagement'
                elif 'performance' in clean_report_name:
                    report_type = 'performance'
                else:
                    report_type = 'analytics'
                
                # Create S3 key compatible with existing ETL pipeline
                s3_key = f"appstore/raw/{report_type}/dt={date_str}/app_id={app_id}/{clean_report_name}_{segment_id}_{timestamp}.csv"
                
                # Upload to S3
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=text_content.encode('utf-8'),
                    ContentType='text/csv',
                    Metadata={
                        'report_name': report_name,
                        'app_id': app_id,
                        'instance_id': instance_id,
                        'segment_id': segment_id,
                        'extraction_date': datetime.now().isoformat(),
                        'row_count': str(result['rows'])
                    }
                )
                
                result['success'] = True
                logger.info(f"   ✅ Saved: {s3_key} ({result['rows']} rows)")
                
            else:
                logger.warning(f"   ⚠️ Empty or invalid CSV data")
                
        except Exception as e:
            logger.error(f"   ❌ Download failed: {str(e)}")
        
        return result
    
    def extract_app_business_data(self, app_id: str, use_ongoing: bool = True) -> Dict:
        """Extract business-critical data for one app
        
        Args:
            app_id: The Apple App ID
            use_ongoing: If True, use ONGOING requests (preferred for daily ETL)
                        If False, use ONE_TIME_SNAPSHOT (for backfills)
        """
        logger.info(f"🔄 Extracting business data for app {app_id}")
        
        result = {
            'app_id': app_id,
            'reports_processed': 0,
            'reports_with_data': 0,
            'total_instances': 0,
            'files_downloaded': 0,
            'total_rows': 0,
            'success': False,
            'report_details': []
        }
        
        # Get or create request ID - prefer ONGOING for daily ETL
        if use_ongoing:
            request_id = self.requestor.create_or_reuse_ongoing_request(app_id)
            if not request_id:
                logger.warning(f"⚠️ Could not get ONGOING request for {app_id}, trying ONE_TIME_SNAPSHOT")
                request_id = self.requestor._load_request_registry(app_id, "ONE_TIME_SNAPSHOT")
        else:
            request_id = self.requestor._load_request_registry(app_id, "ONE_TIME_SNAPSHOT")
            
        if not request_id:
            result['error'] = 'No registered request found and could not create ONGOING'
            return result
        
        # Get all reports
        reports_url = f"{self.requestor.api_base}/analyticsReportRequests/{request_id}/reports"
        response = self.requestor._asc_request('GET', reports_url, timeout=30)
        
        if response.status_code != 200:
            result['error'] = f'Failed to get reports: {response.status_code}'
            return result
        
        data = response.json()
        all_reports = data.get('data', [])
        
        # Filter for target business reports
        business_reports = []
        for report in all_reports:
            report_name = report['attributes']['name']
            if report_name in self.target_report_names:
                business_reports.append(report)
        
        logger.info(f"📊 App {app_id}: Found {len(business_reports)} business reports")
        
        # Process each business report
        for report in business_reports:
            try:
                report_id = report['id']
                report_name = report['attributes']['name']
                report_category = report['attributes']['category']
                
                result['reports_processed'] += 1
                self.extraction_stats['total_reports_checked'] += 1
                
                # Get instances for this report
                instances_url = f"{self.requestor.api_base}/analyticsReports/{report_id}/instances"
                instances_response = self.requestor._asc_request('GET', instances_url, timeout=20)
                
                if instances_response.status_code == 200:
                    instances_data = instances_response.json()
                    instances = instances_data.get('data', [])
                    
                    if len(instances) > 0:
                        result['reports_with_data'] += 1
                        result['total_instances'] += len(instances)
                        self.extraction_stats['reports_with_data'] += 1
                        self.extraction_stats['total_instances'] += len(instances)
                        
                        logger.info(f"   📦 {report_name}: {len(instances)} instances")
                        
                        # Extract data from each instance
                        report_files = 0
                        report_rows = 0
                        
                        for instance in instances:
                            instance_id = instance['id']
                            instance_attrs = instance.get('attributes', {})
                            
                            instance_result = self.extract_instance_data(
                                instance_id, instance_attrs, report_name, app_id
                            )
                            
                            report_files += instance_result['files_downloaded']
                            report_rows += instance_result['total_rows']
                        
                        result['files_downloaded'] += report_files
                        result['total_rows'] += report_rows
                        
                        result['report_details'].append({
                            'name': report_name,
                            'category': report_category,
                            'instances': len(instances),
                            'files': report_files,
                            'rows': report_rows
                        })
                        
                        if report_files > 0:
                            logger.info(f"   ✅ {report_name}: {report_files} files, {report_rows} rows")
                        
                    else:
                        logger.info(f"   ⚪ {report_name}: No instances")
                        
                else:
                    logger.warning(f"   ❌ {report_name}: Instance check failed {instances_response.status_code}")
                    
            except Exception as e:
                error_msg = f"Report {report_name} failed: {str(e)}"
                logger.error(f"   ❌ {error_msg}")
                self.extraction_stats['errors'].append(error_msg)
        
        # Determine success
        if result['files_downloaded'] > 0:
            result['success'] = True
            logger.info(f"✅ App {app_id}: {result['files_downloaded']} files, {result['total_rows']} rows")
        
        return result
    
    def print_extraction_summary(self, app_results: List[Dict]):
        """Print comprehensive extraction summary"""
        successful_apps = [r for r in app_results if r['success']]
        
        print("\n" + "=" * 70)
        print("🎊 FOCUSED APPLE ANALYTICS EXTRACTION SUMMARY")
        print("=" * 70)
        
        print(f"📱 Apps Processed: {len(app_results)}")
        print(f"✅ Apps with Data: {len(successful_apps)}")
        print(f"📊 Reports Checked: {self.extraction_stats['total_reports_checked']}")
        print(f"🎯 Reports with Data: {self.extraction_stats['reports_with_data']}")
        print(f"📦 Total Instances: {self.extraction_stats['total_instances']}")
        print(f"📁 Files Downloaded: {self.extraction_stats['files_downloaded']}")
        print(f"📈 Total Rows: {self.extraction_stats['total_rows']:,}")
        
        if len(successful_apps) > 0:
            print(f"\n🎉 SUCCESS! Extracted data from {len(successful_apps)} apps")
            
            # Show breakdown by report type
            report_summary = {}
            for app_result in successful_apps:
                for report_detail in app_result.get('report_details', []):
                    report_name = report_detail['name']
                    if report_name not in report_summary:
                        report_summary[report_name] = {'files': 0, 'rows': 0, 'apps': 0}
                    
                    report_summary[report_name]['files'] += report_detail['files']
                    report_summary[report_name]['rows'] += report_detail['rows']
                    if report_detail['files'] > 0:
                        report_summary[report_name]['apps'] += 1
            
            print(f"\n📊 DATA BY REPORT TYPE:")
            for report_name, stats in report_summary.items():
                if stats['files'] > 0:
                    print(f"   📁 {report_name}: {stats['files']} files, {stats['rows']} rows from {stats['apps']} apps")


def main():
    """Main execution function"""
    print("🎯 FOCUSED APPLE ANALYTICS EXTRACTION")
    print("=" * 50)
    print("🔍 Targeting business-critical reports with data")
    print("📊 Downloads, Installs, Sessions, Purchases, Engagement\n")
    
    extractor = FocusedAppleDataExtractor()
    
    # Process next batch of apps (excluding already completed ones)
    # Completed so far: 1506168813, 6446987622, 1335964217, 1407833333, 1607293528, 1433884418
    test_app_ids = [
        # Next batch with valid requests (skipping failed ones from previous run)
        '1557244765', '1506886061', '1508131976', '1551104516', '1549247908',
        '1421120099', '1494699802', '1335962714', '1159612010', '1311555637'
    ]
    
    app_results = []
    
    for i, app_id in enumerate(test_app_ids, 1):
        print(f"📱 [{i}/{len(test_app_ids)}] Processing app {app_id}")
        
        try:
            app_result = extractor.extract_app_business_data(app_id)
            app_results.append(app_result)
            
            if i < len(test_app_ids):
                time.sleep(3)  # Pause between apps
                
        except Exception as e:
            logger.error(f"❌ App {app_id} failed: {e}")
            app_results.append({
                'app_id': app_id,
                'success': False,
                'error': str(e)
            })
    
    # Print summary
    extractor.print_extraction_summary(app_results)
    
    # Save results
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = f"focused_extraction_results_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump({
            'extraction_stats': extractor.extraction_stats,
            'app_results': app_results
        }, f, indent=2)
    
    print(f"\n📄 Results saved to {results_file}")
    
    return len([r for r in app_results if r['success']]) > 0


if __name__ == "__main__":
    exit(0 if main() else 1)
