#!/usr/bin/env python3
"""
Apple Analytics Data Freshness Monitor
=======================================

Answers the question: "Did all apps get fresh data today?"

This script validates that the ETL pipeline is working by:
1. Checking S3 for new raw files (by processing_date)
2. Checking S3 for new curated files (by metric_date)
3. Reporting which apps got fresh data vs stale data
4. Flagging apps that may have issues
5. Sending comprehensive reports to Slack

Usage:
    python3 monitor_data_freshness.py                    # Check yesterday's data
    python3 monitor_data_freshness.py --date 2026-01-15  # Check specific date
    python3 monitor_data_freshness.py --days 7           # Check last 7 days
    python3 monitor_data_freshness.py --slack            # Send report to Slack
"""

import os
import sys
import json
import argparse
import boto3
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Optional, Any, Tuple
from collections import defaultdict

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))


class DataFreshnessMonitor:
    """Monitor data freshness across all apps and data types"""
    
    def __init__(self):
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.bucket = os.getenv('S3_BUCKET', 'skidos-apptrack')
        self.app_ids = self._get_app_ids()
        self.data_types = ['downloads', 'engagement', 'sessions', 'installs', 'purchases']
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')

    def _get_app_ids(self) -> List[str]:
        """Get list of all app IDs from environment"""
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            return [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        return ['1506886061']
    
    def check_raw_data_freshness(self, processing_date: str) -> Dict:
        """Check if new raw files were created for a processing date
        
        Returns:
            {
                'app_id': {
                    'downloads': {'files': 5, 'latest_modified': '2026-01-15T09:45:32Z'},
                    'sessions': {'files': 3, 'latest_modified': '2026-01-15T09:46:18Z'},
                    ...
                }
            }
        """
        results = defaultdict(lambda: defaultdict(dict))
        
        for app_id in self.app_ids:
            for data_type in self.data_types:
                prefix = f'appstore/raw/{data_type}/dt={processing_date}/app_id={app_id}/'
                
                try:
                    response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                    files = [obj for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
                    
                    if files:
                        latest_file = max(files, key=lambda x: x['LastModified'])
                        results[app_id][data_type] = {
                            'files': len(files),
                            'latest_modified': latest_file['LastModified'].isoformat(),
                            'size_bytes': sum(f['Size'] for f in files)
                        }
                except Exception as e:
                    results[app_id][data_type] = {'error': str(e)}
        
        return dict(results)
    
    def check_curated_data_freshness(self, metric_date: str) -> Dict:
        """Check if curated parquet files exist for a metric date
        
        Returns:
            {
                'app_id': {
                    'downloads': {'exists': True, 'size_bytes': 12345, 'last_modified': '...'},
                    ...
                }
            }
        """
        results = defaultdict(lambda: defaultdict(dict))
        
        for app_id in self.app_ids:
            for data_type in self.data_types:
                key = f'appstore/curated/{data_type}/dt={metric_date}/app_id={app_id}/data.parquet'
                
                try:
                    response = self.s3.head_object(Bucket=self.bucket, Key=key)
                    results[app_id][data_type] = {
                        'exists': True,
                        'size_bytes': response['ContentLength'],
                        'last_modified': response['LastModified'].isoformat()
                    }
                except self.s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        results[app_id][data_type] = {'exists': False}
                    else:
                        results[app_id][data_type] = {'error': str(e)}
        
        return dict(results)
    
    def check_registry_status(self) -> Dict:
        """Check registry for all apps
        
        Returns:
            {
                'app_id': {
                    'request_id': 'xxx',
                    'created_at': '2025-11-28T...',
                    'age_days': 47,
                    'last_verified': '2026-01-15T...'
                }
            }
        """
        results = {}
        
        for app_id in self.app_ids:
            key = f'appstore/registry/ongoing_requests/{app_id}.json'
            
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
                
                created_at = datetime.fromisoformat(data['created_at'].replace('Z', '+00:00'))
                age_days = (datetime.now(timezone.utc) - created_at).days
                
                results[app_id] = {
                    'request_id': data['request_id'],
                    'created_at': data['created_at'],
                    'age_days': age_days,
                    'last_verified': data.get('last_verified', 'Never'),
                    'app_name': data.get('app_name', 'Unknown')
                }
            except self.s3.exceptions.NoSuchKey:
                results[app_id] = {'exists': False}
            except Exception as e:
                results[app_id] = {'error': str(e)}
        
        return results
    
    def send_to_slack(self, message: str, blocks: Optional[List[Dict[str, Any]]] = None) -> bool:
        """Send message to Slack webhook
        
        Args:
            message: Plain text message (fallback)
            blocks: Rich formatting blocks (optional)
        
        Returns:
            True if successful, False otherwise
        """
        if not self.slack_webhook_url:
            print("⚠️  SLACK_WEBHOOK_URL not configured, skipping Slack notification")
            return False
        
        payload: Dict[str, Any] = {"text": message}
        if blocks:
            payload["blocks"] = blocks
        
        try:
            response = requests.post(
                self.slack_webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'}
            )
            if response.status_code == 200:
                print("✅ Slack notification sent successfully")
                return True
            else:
                print(f"❌ Slack notification failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Failed to send Slack notification: {e}")
            return False
    
    def format_slack_report(self, report: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]]]:
        """Format report data for Slack
        
        Returns:
            (message, blocks) tuple for Slack API
        """
        processing_date = report['report_date']
        metric_date = report['metric_date']
        total_apps = report['total_apps']
        fresh_apps = report['apps_with_fresh_curated_data']
        success_rate = (fresh_apps / total_apps * 100) if total_apps > 0 else 0
        
        # Determine overall status
        if success_rate >= 80:
            status_emoji = "✅"
            status_text = "HEALTHY"
        elif success_rate >= 50:
            status_emoji = "⚠️"
            status_text = "DEGRADED"
        else:
            status_emoji = "❌"
            status_text = "CRITICAL"
        
        # Plain text fallback
        message = f"{status_emoji} Apple Analytics ETL - {status_text}\nSuccess Rate: {fresh_apps}/{total_apps} apps ({success_rate:.1f}%)\nDate: {processing_date}"
        
        # Rich blocks formatting
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} Apple Analytics ETL Report - {processing_date}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Status:*\n{status_text}"},
                    {"type": "mrkdwn", "text": f"*Success Rate:*\n{fresh_apps}/{total_apps} apps ({success_rate:.1f}%)"},
                    {"type": "mrkdwn", "text": f"*Processing Date:*\n{processing_date}"},
                    {"type": "mrkdwn", "text": f"*Metric Date:*\n{metric_date}"}
                ]
            },
            {"type": "divider"}
        ]
        
        # Data type breakdown
        data_type_text = "*📊 Data Type Coverage:*\n"
        for data_type in ['downloads', 'engagement', 'sessions', 'installs', 'purchases']:
            stats = report['data_by_type'][data_type]
            total = stats['fresh'] + stats['missing']
            pct = (stats['fresh'] / total * 100) if total > 0 else 0
            emoji = "✅" if pct >= 80 else "⚠️" if pct >= 50 else "❌"
            data_type_text += f"{emoji} `{data_type:12s}` {stats['fresh']:2d}/{total:2d} ({pct:5.1f}%)\n"
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": data_type_text}
        })
        
        # Registry health
        registry_ages = []
        for details in report['details'].values():
            registry = details.get('registry', {})
            if 'age_days' in registry:
                registry_ages.append(registry['age_days'])
        
        if registry_ages:
            avg_age = sum(registry_ages) / len(registry_ages)
            max_age = max(registry_ages)
            registry_emoji = "✅" if avg_age <= 90 else "⚠️" if avg_age <= 150 else "❌"
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{registry_emoji} Registry Health:*\n• Average age: {avg_age:.1f} days\n• Oldest: {max_age} days\n• Registries: {len(registry_ages)}/{total_apps}"
                }
            })
        
        # Failed apps (if any)
        if report['apps_missing_data']:
            missing_count = len(report['apps_missing_data'])
            if missing_count <= 10:
                # Show all failed apps
                failed_text = f"*⚠️ Apps Missing Data ({missing_count}):*\n"
                for app_id in report['apps_missing_data'][:10]:
                    details = report['details'][app_id]
                    missing_types = ', '.join(details['missing_types'][:3])
                    failed_text += f"• `{app_id}` - Missing: {missing_types}\n"
            else:
                # Show top 5
                failed_text = f"*⚠️ Apps Missing Data ({missing_count}):*\n"
                for app_id in report['apps_missing_data'][:5]:
                    details = report['details'][app_id]
                    missing_types = ', '.join(details['missing_types'][:3])
                    failed_text += f"• `{app_id}` - Missing: {missing_types}\n"
                failed_text += f"_...and {missing_count - 5} more apps_\n"
            
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": failed_text}
            })
        
        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }
            ]
        })
        
        return message, blocks
    
    def generate_daily_report(self, processing_date: str, metric_date: str, send_slack: bool = False) -> Dict:
        """Generate comprehensive daily report
        
        Args:
            processing_date: Date the ETL ran (when data was extracted)
            metric_date: Date of the actual metrics (usually processing_date - 1)
        """
        print("=" * 80)
        print(f"📊 DATA FRESHNESS REPORT - {processing_date}")
        print("=" * 80)
        
        # Check all data sources
        raw_data = self.check_raw_data_freshness(processing_date)
        curated_data = self.check_curated_data_freshness(metric_date)
        registry_data = self.check_registry_status()
        
        # Analyze results
        report = {
            'report_date': processing_date,
            'metric_date': metric_date,
            'total_apps': len(self.app_ids),
            'apps_with_fresh_raw_data': 0,
            'apps_with_fresh_curated_data': 0,
            'apps_missing_data': [],
            'apps_with_errors': [],
            'registry_status': {},
            'data_by_type': defaultdict(lambda: {'fresh': 0, 'missing': 0}),
            'details': {}
        }
        
        # Analyze each app
        for app_id in self.app_ids:
            app_details = {
                'app_id': app_id,
                'raw_files': 0,
                'curated_files': 0,
                'missing_types': [],
                'registry': registry_data.get(app_id, {})
            }
            
            # Check raw data
            app_raw = raw_data.get(app_id, {})
            for data_type in self.data_types:
                if data_type in app_raw and 'files' in app_raw[data_type]:
                    app_details['raw_files'] += app_raw[data_type]['files']
                    report['data_by_type'][data_type]['fresh'] += 1
                else:
                    report['data_by_type'][data_type]['missing'] += 1
            
            # Check curated data
            app_curated = curated_data.get(app_id, {})
            for data_type in self.data_types:
                if app_curated.get(data_type, {}).get('exists'):
                    app_details['curated_files'] += 1
                else:
                    app_details['missing_types'].append(data_type)
            
            # Count freshness
            if app_details['raw_files'] > 0:
                report['apps_with_fresh_raw_data'] += 1
            
            if app_details['curated_files'] > 0:
                report['apps_with_fresh_curated_data'] += 1
            
            if app_details['curated_files'] == 0:
                report['apps_missing_data'].append(app_id)
            
            report['details'][app_id] = app_details
        
        # Print summary
        self._print_report(report)
        
        # Save to file and optionally send to Slack
        self._save_report(report, processing_date, send_slack)
        
        return report
    
    def _print_report(self, report: Dict):
        """Print formatted report to console"""
        print(f"\n📅 Processing Date: {report['report_date']}")
        print(f"📈 Metric Date: {report['metric_date']}")
        print(f"📱 Total Apps: {report['total_apps']}")
        print()
        
        # Overall freshness
        print("=" * 80)
        print("🔍 DATA FRESHNESS SUMMARY")
        print("=" * 80)
        print(f"✅ Apps with fresh RAW data:     {report['apps_with_fresh_raw_data']}/{report['total_apps']} ({report['apps_with_fresh_raw_data']/report['total_apps']*100:.1f}%)")
        print(f"✅ Apps with fresh CURATED data: {report['apps_with_fresh_curated_data']}/{report['total_apps']} ({report['apps_with_fresh_curated_data']/report['total_apps']*100:.1f}%)")
        
        if report['apps_missing_data']:
            print(f"\n⚠️  Apps MISSING data: {len(report['apps_missing_data'])}")
            for app_id in report['apps_missing_data']:
                details = report['details'][app_id]
                registry = details.get('registry', {})
                print(f"   - {app_id} (registry age: {registry.get('age_days', 'N/A')} days, missing: {', '.join(details['missing_types'])})")
        
        # Data type breakdown
        print("\n" + "=" * 80)
        print("📊 DATA TYPE BREAKDOWN")
        print("=" * 80)
        for data_type in ['downloads', 'engagement', 'sessions', 'installs', 'purchases']:
            stats = report['data_by_type'][data_type]
            total = stats['fresh'] + stats['missing']
            pct = (stats['fresh'] / total * 100) if total > 0 else 0
            status = "✅" if pct >= 80 else "⚠️" if pct >= 50 else "❌"
            print(f"{status} {data_type:15s}: {stats['fresh']:2d}/{total:2d} apps ({pct:5.1f}%)")
        
        # Registry status
        print("\n" + "=" * 80)
        print("📖 REGISTRY STATUS")
        print("=" * 80)
        
        registry_ages = []
        for app_id, details in report['details'].items():
            registry = details.get('registry', {})
            if 'age_days' in registry:
                registry_ages.append(registry['age_days'])
        
        if registry_ages:
            avg_age = sum(registry_ages) / len(registry_ages)
            max_age = max(registry_ages)
            min_age = min(registry_ages)
            print(f"Average registry age: {avg_age:.1f} days")
            print(f"Oldest registry:      {max_age} days")
            print(f"Newest registry:      {min_age} days")
            
            # Age distribution
            age_buckets = {'0-30d': 0, '31-60d': 0, '61-90d': 0, '90+d': 0}
            for age in registry_ages:
                if age <= 30:
                    age_buckets['0-30d'] += 1
                elif age <= 60:
                    age_buckets['31-60d'] += 1
                elif age <= 90:
                    age_buckets['61-90d'] += 1
                else:
                    age_buckets['90+d'] += 1
            
            print("\nAge distribution:")
            for bucket, count in age_buckets.items():
                print(f"  {bucket}: {count} apps")
        
        print("\n" + "=" * 80)
    
    def _save_report(self, report: Dict, processing_date: str, send_slack: bool = False):
        """Save report to file and optionally send to Slack"""
        os.makedirs('logs', exist_ok=True)
        filename = f'logs/data_freshness_{processing_date}.json'
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"📄 Report saved to: {filename}")
        
        # Send to Slack if configured
        if send_slack and self.slack_webhook_url:
            message, blocks = self.format_slack_report(report)
            self.send_to_slack(message, blocks)
    
    def check_multi_day_trends(self, days: int = 7) -> Dict:
        """Check data freshness trends over multiple days"""
        print("=" * 80)
        print(f"📈 {days}-DAY DATA FRESHNESS TREND")
        print("=" * 80)
        
        trends = {}
        today = datetime.now(timezone.utc)
        
        for i in range(days):
            date = (today - timedelta(days=i+1)).strftime('%Y-%m-%d')
            raw_data = self.check_raw_data_freshness(date)
            curated_data = self.check_curated_data_freshness(date)
            
            # Count apps with data
            apps_with_raw = sum(1 for app in raw_data.values() if any('files' in v for v in app.values()))
            apps_with_curated = sum(1 for app in curated_data.values() if any(v.get('exists') for v in app.values()))
            
            trends[date] = {
                'apps_with_raw': apps_with_raw,
                'apps_with_curated': apps_with_curated,
                'success_rate': (apps_with_curated / len(self.app_ids) * 100) if self.app_ids else 0
            }
            
            status = "✅" if trends[date]['success_rate'] >= 80 else "⚠️" if trends[date]['success_rate'] >= 50 else "❌"
            print(f"{status} {date}: {apps_with_curated}/{len(self.app_ids)} apps ({trends[date]['success_rate']:.1f}%)")
        
        print("=" * 80)
        return trends


def main():
    parser = argparse.ArgumentParser(description='Monitor Apple Analytics data freshness')
    parser.add_argument('--date', type=str, help='Processing date to check (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='Check trends over N days')
    parser.add_argument('--slack', action='store_true', help='Send report to Slack')
    
    args = parser.parse_args()
    
    monitor = DataFreshnessMonitor()
    
    if args.days:
        # Multi-day trend analysis
        monitor.check_multi_day_trends(args.days)
    else:
        # Single day report
        if args.date:
            processing_date = args.date
        else:
            # Default: yesterday
            processing_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Metric date is typically processing_date - 1
        metric_date = (datetime.strptime(processing_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Generate report with optional Slack notification
        monitor.generate_daily_report(processing_date, metric_date, send_slack=args.slack)


if __name__ == '__main__':
    main()
