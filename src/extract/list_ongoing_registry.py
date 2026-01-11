#!/usr/bin/env python3
"""List Apple Analytics ONGOING request registry entries stored in S3.

Registry layout is defined in `Apple-Analytics/src/extract/apple_analytics_client.py`:
  s3://<bucket>/analytics_requests/registry/app_id=<APP_ID>/<access_type>.json

This script lists all `ongoing.json` registry objects and outputs:
  app_id, request_id, created_at, s3_uri

Note: The registry stores the request_id and timestamp; it does NOT store live request state.
To determine which requests are still running, take the request_id(s) and query Apple API.

Usage (zsh):
  python Apple-Analytics/src/extract/list_ongoing_registry.py --bucket skidos-apptrack

Optional:
  --prefix analytics_requests/registry/
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Iterator, Optional

import boto3


APP_ID_RE = re.compile(r"analytics_requests/registry/app_id=([^/]+)/ongoing\.json$")


def iter_keys(s3_client, bucket: str, prefix: str) -> Iterator[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            yield obj["Key"]


def parse_app_id_from_key(key: str) -> Optional[str]:
    m = APP_ID_RE.search(key)
    return m.group(1) if m else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="analytics_requests/registry/")
    args = ap.parse_args()

    s3 = boto3.client("s3")

    for key in iter_keys(s3, args.bucket, args.prefix):
        if not key.endswith("/ongoing.json"):
            continue

        app_id = parse_app_id_from_key(key) or ""
        s3_uri = f"s3://{args.bucket}/{key}"
        try:
            obj = s3.get_object(Bucket=args.bucket, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            request_id = data.get("request_id", "")
            created_at = data.get("created_at", "")
        except Exception:
            request_id = ""
            created_at = ""

        print("\t".join([app_id, str(request_id), str(created_at), s3_uri]))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
