#!/usr/bin/env python3
"""Synthesize additional daily snapshots from the single real 2021-04-23 feed.

A price-intelligence product needs *history* — Auto Loader incremental ingestion,
price-change tracking and SCD2 only become meaningful across multiple days. We
only have one real snapshot, so this script produces clearly-labelled synthetic
days by perturbing the source rows:

  * price: random walk within +/- `max_drift` (default 8%)
  * on-special: small chance to toggle on/off
  * availability: small chance an item flips available <-> unavailable
  * Date column: restamped to the synthetic date

Output mirrors the source filename convention so `land_to_s3.py` can ingest it:
    data/synth/<YYYY-MM-DD> <Retailer> Data.csv

Usage:
    python -m src.simulate.generate_days --start 2021-04-24 --days 7
    python -m src.simulate.generate_days --start 2021-04-24 --days 7 --seed 42

Note: synthetic output is git-ignored (data/synth/). This is for demo/history;
real pipelines would land genuine daily extracts instead.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Per-retailer config: the source file, its price column, and special/availability
# columns (None where the feed has no such column).
RETAILERS = {
    "Aldi":       dict(price="Price", special=None,        avail=None,
                       date_col="Date"),
    "Coles":      dict(price="Current Price", special="On Special", avail="Availability",
                       date_col="Date", prev_price="Previous Price"),
    "IGA":        dict(price="Price", special=None,        avail=None,
                       date_col="Date"),
    "WOW":        dict(price="Price", special="Specials",  avail="Availability",
                       date_col="Date"),
}


def find_source(source_dir: Path, retailer: str) -> Path | None:
    matches = list(source_dir.glob(f"*{retailer} Data.csv"))
    return matches[0] if matches else None


def perturb_price(raw: str, rng: random.Random, max_drift: float) -> str:
    raw = (raw or "").strip()
    if not raw:
        return raw  # preserve genuine blanks (e.g. WOW unavailable items)
    try:
        value = float(raw.replace("$", "").replace(",", ""))
    except ValueError:
        return raw
    factor = 1.0 + rng.uniform(-max_drift, max_drift)
    return f"{round(value * factor, 2)}"


def generate_day(src: Path, dst: Path, cfg: dict, snapshot: str,
                 rng: random.Random, max_drift: float) -> int:
    rows_out = 0
    with src.open(newline="", encoding="utf-8-sig") as fin:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames or []
        with dst.open("w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                # price drift (and keep Coles Previous Price = yesterday's price)
                if cfg.get("prev_price") and cfg["prev_price"] in row:
                    row[cfg["prev_price"]] = row.get(cfg["price"], "")
                if cfg["price"] in row:
                    row[cfg["price"]] = perturb_price(row[cfg["price"]], rng, max_drift)

                # toggle on-special occasionally
                sp = cfg.get("special")
                if sp and sp in row and rng.random() < 0.10:
                    cur = (row[sp] or "").strip().lower()
                    if sp == "On Special":
                        row[sp] = "" if cur in {"true", "1", "yes"} else "True"
                    else:  # WOW free-text specials
                        row[sp] = "" if cur else "Save $1.00"

                # flip availability occasionally
                av = cfg.get("avail")
                if av and av in row and rng.random() < 0.03:
                    cur = (row[av] or "").strip().lower()
                    row[av] = "Unavailable" if cur == "available" else "Available"

                # restamp the date
                if cfg["date_col"] in row:
                    row[cfg["date_col"]] = snapshot
                writer.writerow(row)
                rows_out += 1
    return rows_out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-dir", default="data/raw", type=Path)
    ap.add_argument("--out-dir", default="data/synth", type=Path)
    ap.add_argument("--start", default="2021-04-24",
                    help="First synthetic date (YYYY-MM-DD)")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--max-drift", type=float, default=0.08)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    total = 0
    for d in range(args.days):
        snapshot: date = start + timedelta(days=d)
        snap_str = snapshot.isoformat()
        for retailer, cfg in RETAILERS.items():
            src = find_source(args.source_dir, retailer)
            if not src:
                print(f"  ! no source for {retailer}, skipping")
                continue
            dst = args.out_dir / f"{snap_str} {retailer} Data.csv"
            n = generate_day(src, dst, cfg, snap_str, rng, args.max_drift)
            total += n
            print(f"  {snap_str} {retailer}: {n} rows -> {dst}")
    print(f"done: generated {total} rows across {args.days} day(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
