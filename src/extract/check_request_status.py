#!/usr/bin/env python3
"""
Check Apple Analytics Request Status
Tests if yesterday's requests are now ready for data extraction
"""

import sys
import json
import os
from pathlib import Path

# Import the Apple Analytics client
from .apple_analytics_client import AppleAnalyticsRequestor

def check_request_status():
    """Check if our registered requests are ready"""
    print("🍎 CHECKING APPLE ANALYTICS REQUEST STATUS")
    print("=" * 60)
    
    requestor = AppleAnalyticsRequestor()
    
    # Known request ID from yesterday
    request_id = "713ba0f0-47b7-4753-b6be-a0a81dbeb81d"
    app_id = "1506886061"
    
    print(f"📱 App ID: {app_id}")
    print(f"🔍 Request ID: {request_id}")
    print(f"📅 Created: Yesterday (2025-11-24)")
    print()
    
    # Check request status
    print("🔄 Checking request completion status...")
    
    try:
        # Use the existing polling method but with just 1 poll to check status
        url = f"{requestor.api_base}/analyticsReportRequests/{request_id}"
        response = requestor._asc_request('GET', url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            attrs = data['data']['attributes']
            
            # Schema-tolerant status extraction
            status = attrs.get('status') or attrs.get('state')
            
            if status:
                print(f"📊 Request Status: {status}")
                
                if status == 'COMPLETED':
                    print("✅ REQUEST IS READY FOR DATA DOWNLOAD!")
                    
                    # Try to download data immediately
                    print("\n📥 Attempting to download analytics files...")
                    files_downloaded = requestor.download_analytics_files(request_id, app_id)
                    
                    if files_downloaded > 0:
                        print(f"🎉 SUCCESS: Downloaded {files_downloaded} analytics files!")
                        return True
                    else:
                        print("⚠️ No files were downloaded - may need investigation")
                        return False
                        
                elif status == 'FAILED':
                    print("❌ REQUEST FAILED - Need to create a new one")
                    return False
                    
                elif status in ['PROCESSING', 'SCHEDULED']:
                    print("⏳ Still processing - check again later")
                    return False
                else:
                    print(f"❓ Unknown status: {status}")
                    return False
            else:
                print("⚠️ No status field found, trying instance check...")
                
                # Fallback: Check if instances are available
                instances_ready = requestor._check_instances_availability(request_id)
                if instances_ready:
                    print("✅ REQUEST IS READY (via instance check)!")
                    
                    # Try to download data
                    print("\n📥 Attempting to download analytics files...")
                    files_downloaded = requestor.download_analytics_files(request_id, app_id)
                    
                    if files_downloaded > 0:
                        print(f"🎉 SUCCESS: Downloaded {files_downloaded} analytics files!")
                        return True
                    else:
                        print("⚠️ No files were downloaded")
                        return False
                else:
                    print("⏳ Instances not ready yet")
                    return False
        else:
            print(f"❌ API Error: {response.status_code} - {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def check_all_registered_requests():
    """Check all apps that have registered requests"""
    print("\n🔍 CHECKING ALL REGISTERED REQUESTS")
    print("-" * 40)
    
    requestor = AppleAnalyticsRequestor()
    
    # Get all configured app IDs from environment
    from dotenv import load_dotenv
    load_dotenv()
    
    app_ids_str = os.getenv('APP_IDS', '')
    all_app_ids = [
        app_id.strip() for app_id in app_ids_str.split(',') 
        if app_id.strip()
    ]
    
    print(f"📱 Checking {len(all_app_ids)} configured apps...")
    
    ready_requests = []
    pending_requests = []
    failed_requests = []
    no_requests = []
    
    for i, app_id in enumerate(all_app_ids, 1):
        print(f"\n📱 [{i:2d}/{len(all_app_ids)}] Checking app {app_id}...")
        
        # Try to load request from registry
        request_id = requestor._load_request_registry(app_id, "ONE_TIME_SNAPSHOT")
        
        if request_id:
            print(f"   📋 Found registered request: {request_id}")
            
            # Quick status check
            try:
                url = f"{requestor.api_base}/analyticsReportRequests/{request_id}"
                response = requestor._asc_request('GET', url, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    attrs = data['data']['attributes']
                    status = attrs.get('status') or attrs.get('state')
                    created_date = attrs.get('createdDate') or attrs.get('created_date')
                    
                    if status == 'COMPLETED':
                        print(f"   ✅ READY: {status}")
                        ready_requests.append((app_id, request_id, created_date))
                    elif status in ['PROCESSING', 'SCHEDULED', 'IN_PROGRESS']:
                        print(f"   ⏳ Status: {status}")
                        pending_requests.append((app_id, request_id, status, created_date))
                    elif status == 'FAILED':
                        print(f"   ❌ FAILED: {status}")
                        failed_requests.append((app_id, request_id, created_date))
                    else:
                        print(f"   ❓ Unknown status: {status}")
                        pending_requests.append((app_id, request_id, status, created_date))
                else:
                    print(f"   ❌ API Error: {response.status_code}")
            except Exception as e:
                print(f"   ❌ Error: {e}")
        else:
            print("   📝 No registered request found")
            no_requests.append(app_id)
    
    return {
        'ready': ready_requests,
        'pending': pending_requests,
        'failed': failed_requests,
        'no_requests': no_requests
    }

def main():
    """Main execution"""
    # Check the specific request from yesterday
    specific_ready = check_request_status()
    
    # Check all registered requests
    all_requests = check_all_registered_requests()
    
    print(f"\n" + "=" * 60)
    print("📊 COMPREHENSIVE SUMMARY")
    print("=" * 60)
    
    if specific_ready:
        print("✅ Yesterday's request (1506886061) is READY and data downloaded!")
    else:
        print("⏳ Yesterday's request (1506886061) is not ready yet")
    
    # Summary of all requests
    ready_requests = all_requests['ready']
    pending_requests = all_requests['pending']
    failed_requests = all_requests['failed']
    no_requests = all_requests['no_requests']
    
    print(f"\n📊 ALL APPS STATUS:")
    print(f"   ✅ Ready for extraction: {len(ready_requests)} apps")
    print(f"   ⏳ Pending requests: {len(pending_requests)} apps")
    print(f"   ❌ Failed requests: {len(failed_requests)} apps")
    print(f"   📝 No requests: {len(no_requests)} apps")
    
    if ready_requests:
        print(f"\n🎉 APPS READY FOR EXTRACTION:")
        for app_id, request_id, created_date in ready_requests:
            print(f"   📱 App {app_id}: {request_id} (created: {created_date})")
        
        print(f"\n💡 Next step: Run bulk extraction for ready apps")
        print(f"   Command: python3 apple_etl_platform/extract/bulk_ready_extraction.py")
    
    if failed_requests:
        print(f"\n❌ APPS WITH FAILED REQUESTS:")
        for app_id, request_id, created_date in failed_requests:
            print(f"   📱 App {app_id}: {request_id} (created: {created_date})")
        print(f"   💡 These apps need new requests created")
    
    if no_requests:
        print(f"\n📝 APPS WITHOUT REQUESTS ({len(no_requests)} apps):")
        no_request_chunks = [no_requests[i:i+10] for i in range(0, len(no_requests), 10)]
        for chunk in no_request_chunks:
            print(f"   {', '.join(chunk)}")
        print(f"   💡 Run comprehensive request manager to create requests:")
        print(f"   Command: python3 apple_etl_platform/extract/comprehensive_request_manager.py")
    
    if pending_requests:
        print(f"\n⏳ APPS WITH PENDING REQUESTS:")
        for app_id, request_id, status, created_date in pending_requests[:5]:  # Show first 5
            print(f"   📱 App {app_id}: {status} (created: {created_date})")
        if len(pending_requests) > 5:
            print(f"   ... and {len(pending_requests) - 5} more")
    
    return len(ready_requests) > 0

if __name__ == "__main__":
    exit(0 if main() else 1)
