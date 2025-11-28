#!/usr/bin/env python3
"""
Historical Backfill Script - Extract historical data from Apple Analytics API

This script creates ONGOING requests and extracts historical data for all configured apps.
Apple Analytics API provides up to 365 days of historical data.

Usage:
    python3 historical_backfill.py                           # Last 30 days
    python3 historical_backfill.py --days 90                 # Last 90 days
    python3 historical_backfill.py --start 2024-01-01 --end 2024-12-31  # Date range
    python3 historical_backfill.py --app-id 1506886061       # Specific app
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Optional

# Add project paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APPLE_ANALYTICS_DIR = os.path.join(BASE_DIR, 'Apple-Analytics')
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, APPLE_ANALYTICS_DIR)

from dotenv import load_dotenv
# Load both .env files
load_dotenv(os.path.join(BASE_DIR, '.env'))
load_dotenv(os.path.join(APPLE_ANALYTICS_DIR, '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return dates


def run_backfill(app_ids: List[str], dates: List[str]):
    """Run extraction for each date and app"""
    from daily_etl import DailyETL
    
    etl = DailyETL()
    
    total_results = {
        'dates_processed': 0,
        'apps_processed': 0,
        'files_downloaded': 0,
        'errors': []
    }
    
    for date_str in dates:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing date: {date_str}")
        logger.info(f"{'='*60}")
        
        for app_id in app_ids:
            try:
                result = etl.extract_reports_for_date(app_id, date_str)
                
                if result['success']:
                    total_results['apps_processed'] += 1
                    total_results['files_downloaded'] += result.get('files_downloaded', 0)
                else:
                    if result.get('errors'):
                        total_results['errors'].extend(result['errors'])
                        
            except Exception as e:
                logger.error(f"Error processing {app_id}/{date_str}: {e}")
                total_results['errors'].append(f"{app_id}/{date_str}: {str(e)}")
        
        total_results['dates_processed'] += 1
        
        # Progress update
        logger.info(f"Progress: {total_results['dates_processed']}/{len(dates)} dates, "
                   f"{total_results['files_downloaded']} files downloaded")
    
    return total_results


def main():
    parser = argparse.ArgumentParser(description='Historical backfill for Apple Analytics')
    parser.add_argument('--days', type=int, default=30, help='Number of days to backfill (default: 30)')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--app-id', type=str, help='Specific app ID (default: all apps)')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("APPLE ANALYTICS - HISTORICAL BACKFILL")
    print("=" * 70)
    
    # Determine date range
    if args.start and args.end:
        dates = get_date_range(args.start, args.end)
        logger.info(f"Date range: {args.start} to {args.end} ({len(dates)} days)")
    else:
        end_date = datetime.now() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=args.days - 1)
        dates = get_date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        logger.info(f"Last {args.days} days: {dates[0]} to {dates[-1]}")
    
    # Get app IDs
    if args.app_id:
        app_ids = [args.app_id]
    else:
        app_ids_env = os.getenv('APP_IDS', '')
        if app_ids_env:
            app_ids = [aid.strip() for aid in app_ids_env.split(',') if aid.strip()]
        else:
            # Default test apps
            app_ids = ['1506886061', '1159612010', '1468754350']
    
    logger.info(f"Apps to process: {len(app_ids)}")
    logger.info(f"Total extractions: {len(dates) * len(app_ids)}")
    
    # Confirm before proceeding
    print(f"\nThis will extract data for:")
    print(f"  - {len(dates)} dates")
    print(f"  - {len(app_ids)} apps")
    print(f"  - Total: {len(dates) * len(app_ids)} extraction jobs")
    print()
    
    # Run backfill
    try:
        results = run_backfill(app_ids, dates)
        
        print("\n" + "=" * 70)
        print("BACKFILL COMPLETE")
        print("=" * 70)
        print(f"Dates processed: {results['dates_processed']}")
        print(f"Apps processed: {results['apps_processed']}")
        print(f"Files downloaded: {results['files_downloaded']}")
        print(f"Errors: {len(results['errors'])}")
        
        if results['errors']:
            print("\nErrors encountered:")
            for error in results['errors'][:10]:
                print(f"  - {error}")
            if len(results['errors']) > 10:
                print(f"  ... and {len(results['errors']) - 10} more")
                
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise


if __name__ == '__main__':
    main()
