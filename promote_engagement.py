#!/usr/bin/env python3
"""Promote a verified engagement rebuild from a staging prefix into production.

  * every staging partition (dt=<date>/app_id=<app>/data.parquet) is copied over the
    production one;
  * from --delete-from onward (the range verified day-by-day against App Store Connect),
    a production partition of a rebuilt app that the rebuild did not produce is stale -
    an old WEEKLY/MONTHLY roll-up written onto a day with no daily data - and is deleted.
Earlier dates are only overwritten, never deleted.

    python3 promote_engagement.py --staging appstore/staging/x/ --delete-from 2026-01-01 --dry-run
    python3 promote_engagement.py --staging appstore/staging/x/ --delete-from 2026-01-01
"""
import argparse
from concurrent.futures import ThreadPoolExecutor

import boto3
from dotenv import load_dotenv

load_dotenv('/data/apple-analytics/.env')
BUCKET = 'skidos-apptrack'
PROD = 'appstore/curated/engagement/'


def partitions(s3, prefix):
    out = {}
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get('Contents', []):
            rest = o['Key'][len(prefix):]            # dt=YYYY-MM-DD/app_id=NNN/data.parquet
            if rest.startswith('dt=') and '/app_id=' in rest:
                ds = rest[3:13]
                app = rest.split('/app_id=')[1].split('/')[0]
                out[(ds, app)] = o['Key']
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--staging', required=True)
    ap.add_argument('--delete-from', required=True)
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    s3 = boto3.client('s3', region_name='us-east-1')
    staged, prod = partitions(s3, args.staging), partitions(s3, PROD)
    apps = {a for _, a in staged}
    last = max(ds for ds, _ in staged)
    stale = [k for (ds, a), k in prod.items()
             if a in apps and args.delete_from <= ds <= last and (ds, a) not in staged]
    new = sum(1 for k in staged if k not in prod)
    print(f'staging partitions {len(staged)} ({new} new to prod) | prod partitions {len(prod)} | '
          f'stale to delete ({args.delete_from}..{last}): {len(stale)}')
    for k in sorted(stale)[:15]:
        print('  stale:', k)
    if args.dry_run:
        return

    def copy(item):
        (ds, app), key = item
        boto3.client('s3', region_name='us-east-1').copy_object(
            Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': key},
            Key=f'{PROD}dt={ds}/app_id={app}/data.parquet')

    with ThreadPoolExecutor(32) as pool:
        list(pool.map(copy, staged.items()))
    for i in range(0, len(stale), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={'Objects': [{'Key': k} for k in stale[i:i + 1000]]})
    after = partitions(s3, PROD)
    print(f'done: copied {len(staged)}, deleted {len(stale)}; prod partitions now {len(after)}')


if __name__ == '__main__':
    main()
