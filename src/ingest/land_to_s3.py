#!/usr/bin/env python3
"""Land raw supermarket CSVs into the S3 raw zone using a date-partitioned layout.

This is the *extract -> landing* stage. Auto Loader (Bronze) then discovers new
files incrementally under each retailer prefix.

Target key layout (Hive-style date partition so Bronze can derive snapshot_date
from the path):

    s3://<bucket>/<prefix>/<retailer>/date=YYYY-MM-DD/<retailer>_YYYY-MM-DD.csv

Usage:
    python -m src.ingest.land_to_s3 \
        --bucket my-supermarket-raw \
        --source-dir data/raw \
        --prefix raw

    # dry run (no upload, just print the plan)
    python -m src.ingest.land_to_s3 --bucket my-supermarket-raw --dry-run

Credentials are resolved by boto3 from the standard chain (env vars, ~/.aws,
instance role). Nothing secret is hard-coded.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Map the messy source filenames to a clean retailer slug.
RETAILER_PATTERNS = {
    "aldi": "aldi",
    "coles": "coles",
    "iga": "iga",
    "wow": "woolworths",
    "woolworths": "woolworths",
}

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def classify_file(path: Path) -> tuple[str | None, str | None]:
    """Return ``(retailer_slug, snapshot_date)`` parsed from a filename."""
    name = path.name.lower()
    retailer = next((slug for key, slug in RETAILER_PATTERNS.items() if key in name), None)
    date_match = DATE_RE.search(path.name)
    return retailer, (date_match.group(1) if date_match else None)


def build_key(prefix: str, retailer: str, date: str) -> str:
    return f"{prefix.strip('/')}/{retailer}/date={date}/{retailer}_{date}.csv"


def plan_uploads(source_dir: Path, prefix: str) -> list[tuple[Path, str]]:
    plan: list[tuple[Path, str]] = []
    for csv_path in sorted(source_dir.rglob("*.csv")):
        retailer, date = classify_file(csv_path)
        if not retailer or not date:
            print(f"  ! skipping (cannot classify): {csv_path.name}", file=sys.stderr)
            continue
        plan.append((csv_path, build_key(prefix, retailer, date)))
    return plan


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", required=True, help="Target S3 bucket name")
    ap.add_argument("--source-dir", default="data/raw", type=Path)
    ap.add_argument("--prefix", default="raw", help="Key prefix / raw zone root")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.source_dir.exists():
        print(f"source dir not found: {args.source_dir}", file=sys.stderr)
        return 2

    plan = plan_uploads(args.source_dir, args.prefix)
    if not plan:
        print("nothing to upload", file=sys.stderr)
        return 1

    print(f"Landing {len(plan)} file(s) into s3://{args.bucket}/")
    for path, key in plan:
        print(f"  {path}  ->  s3://{args.bucket}/{key}")

    if args.dry_run:
        print("dry-run: no files uploaded")
        return 0

    import boto3  # lazy import so --dry-run works without boto3 installed

    s3 = boto3.client("s3")
    for path, key in plan:
        s3.upload_file(
            str(path), args.bucket, key,
            ExtraArgs={"ContentType": "text/csv"},
        )
        print(f"  uploaded {key}")
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
