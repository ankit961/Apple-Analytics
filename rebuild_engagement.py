#!/usr/bin/env python3
"""One-off: rebuild appstore/curated/engagement from ALL raw engagement files so every
(app, date) holds the newest DAILY instance Apple has sent for it - including the
corrections Apple re-sends inside later instances - as unified_etl.py now does.

The old curation summed every instance in a processing-date folder: WEEKLY (Monday)
and MONTHLY (1st) roll-ups were added to those days, and restated DAILY copies of a
date were added together whenever they differed.

Raw folders re-download the same Apple segment files every day, so each unique
segment (by id) is read once, from its first download. That download time stands in
for when Apple published it: folder dates are not reliable (backfill runs wrote
into old folder names). Dates that no DAILY file covers are left untouched and are
listed in the coverage report.

    python3 rebuild_engagement.py --dry-run [--app-id 1506886061]      # report only
    python3 rebuild_engagement.py --out-prefix appstore/staging/x/    # write a copy
    python3 rebuild_engagement.py                                     # write curated
"""
import argparse
import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import date, datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import boto3
import pandas as pd
import unified_etl as U

BUCKET = os.getenv('S3_BUCKET', 'skidos-apptrack')
RAW = 'appstore/raw/engagement/'
CURATED = 'appstore/curated/engagement/'

logging.getLogger().setLevel(logging.WARNING)
log = logging.getLogger('rebuild')
log.setLevel(logging.INFO)


def inventory(s3, max_seen=None) -> dict:
    """{app_id: {segment_id: (first download time, key)}} over every raw folder."""
    folders = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=RAW, Delimiter='/'):
        folders += [p['Prefix'] for p in page.get('CommonPrefixes', [])]

    def list_folder(prefix):
        out = []
        for page in boto3.client('s3', region_name='us-east-1').get_paginator('list_objects_v2').paginate(
                Bucket=BUCKET, Prefix=prefix):
            for o in page.get('Contents', []):
                name = o['Key'].split('/')[-1].lower()
                if o['Key'].endswith('.csv') and 'detailed' not in name and 'performance' not in name:
                    out.append((o['Key'], o['LastModified']))
        return out

    segs = {}
    with ThreadPoolExecutor(max_workers=16) as pool:
        for objs in pool.map(list_folder, folders):
            for key, seen in objs:
                if max_seen and seen > max_seen:
                    continue
                app = key.split('app_id=')[1].split('/')[0]
                seg = key.rsplit('_', 1)[1][:-4]
                cur = segs.setdefault(app, {}).get(seg)
                if cur is None or seen < cur[0]:
                    segs[app][seg] = (seen, key)
    return segs


def rebuild_app(app_id: str, segments: dict, out_prefix: str, dry_run: bool, first: str, last: str) -> tuple:
    s3 = boto3.client('s3', region_name='us-east-1')

    def read(item):
        seen, key = item
        resp = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_csv(io.BytesIO(resp['Body'].read()), sep='\t', low_memory=False)
        if df.empty or 'Date' not in df.columns:
            return None
        return {'df': df, 'meta': resp.get('Metadata', {}), 'seen': seen.date().isoformat()}

    with ThreadPoolExecutor(max_workers=8) as pool:
        files = [f for f in pool.map(read, segments.values()) if f is not None]
    if not files:
        return [], (app_id, None, None, 0, 0)
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    selected, stats = U.select_latest_daily_instance(files, today)
    etl = U.UnifiedETL.__new__(U.UnifiedETL)
    curated = etl._transform_dataframe('engagement', selected, app_id, today) if not selected.empty else None
    rows = []
    if curated is not None:
        for ds, part in curated.groupby('dt'):
            if (first and ds < first) or (last and ds > last):
                continue
            if not dry_run:
                buf = io.BytesIO()
                part.to_parquet(buf, engine='pyarrow', compression='snappy', index=False)
                s3.put_object(Bucket=BUCKET, Key=f'{out_prefix}dt={ds}/app_id={app_id}/data.parquet', Body=buf.getvalue())
            e, p = part['event_type'], part['page_type']
            impr = part[((e == 'Impression') & (p == 'No page')) |
                        ((e == 'Page view') & p.isin(['Product page', 'Store sheet', 'In-app event']))]['impressions'].sum()
            ppv = part[(e == 'Page view') & p.isin(['Product page', 'Store sheet'])]['impressions'].sum()
            rows.append((app_id, ds, int(impr), int(ppv)))
    ds_all = sorted(r[1] for r in rows)
    return rows, (app_id, ds_all[0] if ds_all else None, ds_all[-1] if ds_all else None,
                  stats['daily_files'], stats['rollup_files'])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--from', dest='first', help='only write metric dates on/after this')
    ap.add_argument('--to', dest='last', help='only write metric dates on/before this')
    ap.add_argument('--app-id', action='append', help='limit to these apps (repeatable)')
    ap.add_argument('--out-prefix', default=CURATED, help='write here instead (e.g. a staging copy)')
    ap.add_argument('--max-seen', help='ignore files downloaded after this UTC date (e.g. a run still in progress)')
    ap.add_argument('--processes', type=int, default=4)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--report', default=os.path.join(SCRIPT_DIR, 'logs', 'rebuild_engagement_report.csv'))
    args = ap.parse_args()

    max_seen = (datetime.fromisoformat(args.max_seen).replace(tzinfo=timezone.utc) if args.max_seen else None)
    segs = inventory(boto3.client('s3', region_name='us-east-1'), max_seen)
    if args.app_id:
        segs = {a: s for a, s in segs.items() if a in set(args.app_id)}
    log.info(f'{len(segs)} apps, {sum(len(s) for s in segs.values())} unique segment files'
             f'{" (DRY RUN)" if args.dry_run else ""}')

    rows, coverage = [], []
    # Largest apps first so they do not finish last.
    order = sorted(segs, key=lambda a: -len(segs[a]))
    with ProcessPoolExecutor(max_workers=args.processes) as pool:
        futs = {pool.submit(rebuild_app, a, segs[a], args.out_prefix, args.dry_run, args.first, args.last): a
                for a in order}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                r, c = fut.result()
                rows += r
                coverage.append(c)
            except Exception as e:
                log.warning(f'app {futs[fut]}: {type(e).__name__}: {e}')
            if i % 10 == 0:
                log.info(f'{i}/{len(futs)} apps done')

    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    pd.DataFrame(rows, columns=['app_id', 'date', 'impressions', 'product_page_views']).to_csv(args.report, index=False)
    pd.DataFrame(coverage, columns=['app_id', 'first_date', 'last_date', 'daily_files', 'rollup_files']).to_csv(
        args.report.replace('.csv', '_coverage.csv'), index=False)
    log.info(f'{len(rows)} partitions {"computed" if args.dry_run else "written"} for {len(coverage)} apps -> {args.report}')


if __name__ == '__main__':
    main()
