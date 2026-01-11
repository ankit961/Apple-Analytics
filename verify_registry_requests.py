#!/usr/bin/env python3
"""
Verify ONGOING Registry Requests
=================================

This script checks if the ONGOING request IDs stored in S3 registry are actually valid
by querying the Apple Analytics API.

This will help determine if the "Registry ONGOING request is invalid" errors are:
1. Real (Apple expired the requests)
2. False positives (rate limiting causing verification to fail)
"""

import os
import sys
import json
import boto3
from datetime import datetime

# Setup paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

from src.extract.apple_analytics_client import AppleAnalyticsRequestor


def list_registry_entries(bucket: str = 'skidos-apptrack') -> list:
    """List all ONGOING registry entries in S3"""
    s3 = boto3.client('s3')
    entries = []
    
    prefix = 'analytics_requests/registry/'
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('/ongoing.json'):
                    # Extract app_id from path
                    parts = key.split('/')
                    for part in parts:
                        if part.startswith('app_id='):
                            app_id = part.split('=')[1]
                            
                            # Get registry data
                            try:
                                registry_obj = s3.get_object(Bucket=bucket, Key=key)
                                registry_data = json.loads(registry_obj['Body'].read().decode('utf-8'))
                                
                                entries.append({
                                    'app_id': app_id,
                                    'request_id': registry_data.get('request_id'),
                                    'created_at': registry_data.get('created_at'),
                                    's3_key': key
                                })
                            except Exception as e:
                                print(f"Error reading {key}: {e}")
                            break
    except Exception as e:
        print(f"Error listing S3: {e}")
        return []
    
    return entries


def verify_request_with_apple(requestor: AppleAnalyticsRequestor, request_id: str) -> dict:
    """Verify if a request ID is valid with Apple API"""
    try:
        url = f"{requestor.api_base}/analyticsReportRequests/{request_id}"
        response = requestor._asc_request('GET', url, timeout=30)
        
        return {
            'valid': response.status_code == 200,
            'status_code': response.status_code,
            'response': response.text[:500] if response.status_code != 200 else 'OK'
        }
    except Exception as e:
        return {
            'valid': False,
            'status_code': 'ERROR',
            'response': str(e)
        }


def main():
    print("=" * 80)
    print("🔍 VERIFYING ONGOING REGISTRY REQUESTS")
    print("=" * 80)
    
    # Initialize requestor
    requestor = AppleAnalyticsRequestor()
    
    # Get all registry entries
    print("\n📖 Loading registry entries from S3...")
    entries = list_registry_entries()
    
    if not entries:
        print("❌ No registry entries found!")
        return 1
    
    print(f"✅ Found {len(entries)} ONGOING registry entries\n")
    
    # Pick a few apps that showed "invalid" errors in the logs
    # From the logs we saw: 6449359840, 6448038513, 6469684995, 6466577779
    test_apps = ['6449359840', '6448038513', '6469684995', '6466577779', '1506886061']
    
    print("🧪 Testing specific apps that showed 'invalid' errors in logs:")
    print("-" * 80)
    
    valid_count = 0
    invalid_count = 0
    error_count = 0
    
    for app_id in test_apps:
        # Find this app in registry
        entry = next((e for e in entries if e['app_id'] == app_id), None)
        
        if not entry:
            print(f"\n❌ App {app_id}: NOT IN REGISTRY")
            continue
        
        request_id = entry['request_id']
        created_at = entry['created_at']
        
        print(f"\n📱 App {app_id}")
        print(f"   Request ID: {request_id}")
        print(f"   Created:    {created_at}")
        print(f"   Verifying with Apple API...", end='', flush=True)
        
        # Verify with Apple
        import time
        time.sleep(0.5)  # Small delay to avoid rate limiting
        
        result = verify_request_with_apple(requestor, request_id)
        
        if result['valid']:
            print(f" ✅ VALID")
            valid_count += 1
        elif result['status_code'] == 429:
            print(f" ⚠️  RATE LIMITED (429)")
            print(f"   Can't verify - hit rate limit")
            error_count += 1
        else:
            print(f" ❌ INVALID")
            print(f"   Status: {result['status_code']}")
            print(f"   Response: {result['response'][:200]}")
            invalid_count += 1
    
    # Summary
    print("\n" + "=" * 80)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 80)
    print(f"✅ Valid:        {valid_count}")
    print(f"❌ Invalid:      {invalid_count}")
    print(f"⚠️  Rate Limited: {error_count}")
    print("=" * 80)
    
    if error_count > 0:
        print("\n⚠️  WARNING: Some verifications were rate limited.")
        print("   This suggests the _verify_request_exists() function is also")
        print("   being rate limited, causing false 'invalid' detections!")
    
    if valid_count > 0 and invalid_count == 0:
        print("\n✅ CONCLUSION: Registry requests are VALID!")
        print("   The 'invalid' errors are likely caused by rate limiting during verification.")
        print("   Solution: Skip verification or add delays/caching.")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
