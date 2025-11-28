"""
Apple Analytics Request Status Checker

This script checks the status of all pending Apple Analytics requests
and provides detailed information about request processing.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

# Import necessary modules
import requests
import jwt

# Note: dotenv not required in production setup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AppleRequestStatusChecker:
    """Check status of Apple Analytics requests"""
    
    def __init__(self):
        """Initialize the status checker"""
        self.api_base = "https://api.appstoreconnect.apple.com/v1"
        
        # Load credentials from environment or default values
        self.issuer_id = os.getenv('ASC_ISSUER_ID')
        self.key_id = os.getenv('ASC_KEY_ID')  
        self.p8_path = os.getenv('ASC_P8_PATH')
        
        # Parse app IDs (empty default for testing)
        app_ids_str = os.getenv('APP_IDS', '')
        self.all_app_ids = set(
            app_id.strip() for app_id in app_ids_str.split(',') 
            if app_id.strip()
        ) if app_ids_str else set()
        
        # Generate API headers (graceful fallback for testing)
        try:
            if self.issuer_id and self.key_id and self.p8_path:
                self.headers = self._generate_headers()
                logger.info("Apple API initialized successfully")
            else:
                logger.warning("Apple API credentials not provided - running in test mode")
                self.headers = None
        except Exception as e:
            logger.warning(f"Failed to initialize Apple API (test mode): {e}")
            self.headers = None
    
    def _generate_jwt_token(self) -> str:
        """Generate JWT token for Apple API authentication"""
        try:
            if not self.p8_path or not Path(self.p8_path).exists():
                raise ValueError("P8 key file not found or not configured")
                
            with open(self.p8_path, 'r') as f:
                private_key = f.read()
            
            now = datetime.now()
            payload = {
                'iss': self.issuer_id,
                'iat': int(now.timestamp()),
                'exp': int((now + timedelta(minutes=20)).timestamp()),
                'aud': 'appstoreconnect-v1'
            }
            
            headers = {
                'kid': self.key_id,
                'typ': 'JWT',
                'alg': 'ES256'
            }
            
            token = jwt.encode(payload, private_key, algorithm='ES256', headers=headers)
            return token
            
        except Exception as e:
            logger.error(f"Failed to generate JWT token: {e}")
            raise
    
    def _generate_headers(self) -> Dict[str, str]:
        """Generate headers for API requests"""
        token = self._generate_jwt_token()
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    def get_all_analytics_requests(self) -> List[Dict]:
        """Get all analytics requests from Apple API"""
        if not self.headers:
            logger.error("API not initialized")
            return []
        
        try:
            url = f"{self.api_base}/analyticsReportRequests"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json()
            requests_list = data.get('data', [])
            
            logger.info(f"Retrieved {len(requests_list)} analytics requests from Apple API")
            return requests_list
            
        except Exception as e:
            logger.error(f"Error retrieving analytics requests: {e}")
            return []
    
    def analyze_request_status(self, requests_list: List[Dict]) -> Dict:
        """Analyze the status of all requests"""
        analysis = {
            'total_requests': len(requests_list),
            'by_status': {},
            'by_category': {},
            'by_app_id': {},
            'configured_apps': {
                'with_requests': set(),
                'without_requests': set(self.all_app_ids),
                'summary': {}
            },
            'recent_requests': [],
            'oldest_pending': None,
            'newest_request': None
        }
        
        if not requests_list:
            analysis['configured_apps']['without_requests'] = self.all_app_ids
            analysis['configured_apps']['summary'] = {
                'total_configured': len(self.all_app_ids),
                'with_requests': 0,
                'without_requests': len(self.all_app_ids),
                'coverage_percent': 0.0
            }
            return analysis
        
        # Process each request
        for request in requests_list:
            try:
                attributes = request.get('attributes', {})
                
                # Basic categorization
                status = attributes.get('processing_status', 'UNKNOWN')
                category = attributes.get('category', 'UNKNOWN')
                app_id = attributes.get('app_id')
                created_date = attributes.get('created_date')
                
                # Count by status
                analysis['by_status'][status] = analysis['by_status'].get(status, 0) + 1
                
                # Count by category
                analysis['by_category'][category] = analysis['by_category'].get(category, 0) + 1
                
                # Track per app
                if app_id:
                    if app_id not in analysis['by_app_id']:
                        analysis['by_app_id'][app_id] = {
                            'requests': [],
                            'total_count': 0,
                            'by_status': {},
                            'by_category': {}
                        }
                    
                    app_data = analysis['by_app_id'][app_id]
                    app_data['requests'].append(request)
                    app_data['total_count'] += 1
                    app_data['by_status'][status] = app_data['by_status'].get(status, 0) + 1
                    app_data['by_category'][category] = app_data['by_category'].get(category, 0) + 1
                    
                    # Track configured apps
                    if app_id in self.all_app_ids:
                        analysis['configured_apps']['with_requests'].add(app_id)
                        analysis['configured_apps']['without_requests'].discard(app_id)
                
                # Track recent requests (last 7 days)
                if created_date:
                    try:
                        created_dt = datetime.fromisoformat(created_date.replace('Z', '+00:00'))
                        if created_dt > datetime.now() - timedelta(days=7):
                            analysis['recent_requests'].append({
                                'id': request.get('id'),
                                'app_id': app_id,
                                'status': status,
                                'category': category,
                                'created_date': created_date,
                                'created_dt': created_dt
                            })
                        
                        # Track oldest pending and newest overall
                        if status == 'IN_PROGRESS':
                            if not analysis['oldest_pending'] or created_dt < analysis['oldest_pending']['created_dt']:
                                analysis['oldest_pending'] = {
                                    'id': request.get('id'),
                                    'app_id': app_id,
                                    'created_date': created_date,
                                    'created_dt': created_dt,
                                    'category': category
                                }
                        
                        if not analysis['newest_request'] or created_dt > analysis['newest_request']['created_dt']:
                            analysis['newest_request'] = {
                                'id': request.get('id'),
                                'app_id': app_id,
                                'created_date': created_date,
                                'created_dt': created_dt,
                                'status': status,
                                'category': category
                            }
                            
                    except Exception as e:
                        logger.warning(f"Error parsing date {created_date}: {e}")
                        
            except Exception as e:
                logger.error(f"Error processing request: {e}")
                continue
        
        # Sort recent requests by date
        analysis['recent_requests'].sort(key=lambda x: x['created_dt'], reverse=True)
        
        # Generate configured apps summary
        analysis['configured_apps']['summary'] = {
            'total_configured': len(self.all_app_ids),
            'with_requests': len(analysis['configured_apps']['with_requests']),
            'without_requests': len(analysis['configured_apps']['without_requests']),
            'coverage_percent': round(
                (len(analysis['configured_apps']['with_requests']) / len(self.all_app_ids)) * 100, 1
            ) if self.all_app_ids else 0
        }
        
        return analysis
    
    def print_status_report(self, analysis: Dict):
        """Print a formatted status report"""
        print("\n" + "="*80)
        print("APPLE ANALYTICS REQUEST STATUS REPORT")
        print("="*80)
        
        # Overall Summary
        print(f"\n📊 OVERALL SUMMARY:")
        print(f"   • Total Requests in System: {analysis['total_requests']}")
        
        if analysis['by_status']:
            print(f"   • Status Breakdown:")
            for status, count in sorted(analysis['by_status'].items()):
                emoji = "✅" if status == "COMPLETED" else "🔄" if status == "IN_PROGRESS" else "❌"
                print(f"     {emoji} {status}: {count} requests")
        
        # Category Breakdown
        if analysis['by_category']:
            print(f"\n📈 REPORT CATEGORIES:")
            for category, count in sorted(analysis['by_category'].items()):
                print(f"   • {category}: {count} requests")
        
        # Configured Apps Coverage
        summary = analysis['configured_apps']['summary']
        print(f"\n🎯 CONFIGURED APPS COVERAGE:")
        print(f"   • Total Configured Apps: {summary['total_configured']}")
        print(f"   • Apps with Requests: {summary['with_requests']} ({summary['coverage_percent']}%)")
        print(f"   • Apps without Requests: {summary['without_requests']}")
        
        # Recent Activity
        if analysis['recent_requests']:
            print(f"\n🕐 RECENT REQUESTS (Last 7 Days):")
            for req in analysis['recent_requests'][:10]:  # Show top 10
                age_hours = (datetime.now() - req['created_dt'].replace(tzinfo=None)).total_seconds() / 3600
                print(f"   • App {req['app_id']}: {req['status']} ({req['category']}) - {age_hours:.1f}h ago")
        
        # Processing Status
        if analysis['oldest_pending']:
            oldest = analysis['oldest_pending']
            age_hours = (datetime.now() - oldest['created_dt'].replace(tzinfo=None)).total_seconds() / 3600
            print(f"\n⏳ OLDEST PENDING REQUEST:")
            print(f"   • App {oldest['app_id']}: {oldest['category']} - {age_hours:.1f} hours old")
        
        if analysis['newest_request']:
            newest = analysis['newest_request']
            age_hours = (datetime.now() - newest['created_dt'].replace(tzinfo=None)).total_seconds() / 3600
            print(f"\n🆕 NEWEST REQUEST:")
            print(f"   • App {newest['app_id']}: {newest['status']} ({newest['category']}) - {age_hours:.1f}h ago")
        
        # Apps needing requests
        if analysis['configured_apps']['without_requests']:
            missing_apps = sorted(list(analysis['configured_apps']['without_requests']))
            print(f"\n❌ APPS WITHOUT REQUESTS ({len(missing_apps)} apps):")
            missing_chunks = [missing_apps[i:i+10] for i in range(0, len(missing_apps), 10)]
            for chunk in missing_chunks:
                print(f"   {', '.join(chunk)}")
        
        print("\n" + "="*80)
    
    def save_analysis(self, analysis: Dict, filename: Optional[str] = None) -> Path:
        """Save analysis results to file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"request_status_analysis_{timestamp}.json"
        
        # Convert sets to lists for JSON serialization
        analysis_copy = json.loads(json.dumps(analysis, default=str))
        analysis_copy['configured_apps']['with_requests'] = list(analysis['configured_apps']['with_requests'])
        analysis_copy['configured_apps']['without_requests'] = list(analysis['configured_apps']['without_requests'])
        
        filepath = Path(__file__).parent / filename
        
        with open(filepath, 'w') as f:
            json.dump(analysis_copy, f, indent=2, default=str)
        
        logger.info(f"Analysis saved to: {filepath}")
        return filepath


def main():
    """Main function to check request status"""
    try:
        checker = AppleRequestStatusChecker()
        
        if not checker.headers:
            print("❌ Cannot proceed without Apple API initialization")
            return
        
        print("🔍 Checking Apple Analytics request status...")
        
        # Get all requests
        requests_list = checker.get_all_analytics_requests()
        
        # Analyze status
        analysis = checker.analyze_request_status(requests_list)
        
        # Print report
        checker.print_status_report(analysis)
        
        # Save analysis
        analysis_file = checker.save_analysis(analysis)
        
        print(f"\n💾 Detailed analysis saved to: {analysis_file}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        missing_count = analysis['configured_apps']['summary']['without_requests']
        if missing_count > 0:
            print(f"   1. Create requests for {missing_count} missing apps")
        
        pending_count = analysis['by_status'].get('IN_PROGRESS', 0)
        if pending_count > 0:
            print(f"   2. Monitor {pending_count} pending requests")
        
        if analysis['oldest_pending']:
            hours_old = (datetime.now() - analysis['oldest_pending']['created_dt'].replace(tzinfo=None)).total_seconds() / 3600
            if hours_old > 48:
                print(f"   3. Investigate requests pending longer than 48 hours")
        
        print(f"   4. Set up automated daily request creation")
        print(f"   5. Implement request status monitoring")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    main()
