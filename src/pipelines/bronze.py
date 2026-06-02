"""Bronze layer — incremental ingestion of raw supermarket CSVs via Auto Loader.

Part of the Lakeflow Declarative Pipeline (Delta Live Tables). One streaming
table per retailer preserves each feed's native schema verbatim, plus:
  * ``_rescued_data`` — any columns that didn't match the inferred schema,
  * ``_source_file`` / ``_ingest_ts`` — provenance,
  * ``_snapshot_date`` — parsed from the Hive-style ``date=YYYY-MM-DD`` path.

Auto Loader (`cloudFiles`) tracks which files it has already processed, so each
pipeline run ingests only newly-landed files — the realistic daily pattern.

Configuration (set in databricks.yml / pipeline settings):
    source.root  e.g.  s3://my-supermarket-raw/raw   (or a UC Volume path)
"""

import re

import dlt
from pyspark.sql import functions as F

# Resolve the raw-zone root from pipeline config; fall back to a UC Volume so the
# pipeline still runs if external S3 isn't available on Databricks Free Edition.
SOURCE_ROOT = spark.conf.get(  # noqa: F821  (spark provided by DLT runtime)
    "source.root", "/Volumes/supermarket/bronze/landing/raw"
)


def _sanitize_columns(df):
    """Replace characters that Delta Lake forbids in column names ( ,;{}()\\n\\t=)
    with underscores.  Aldi has spaces in headers like 'Unit Price', 'Main Category'.
    Applied to every feed so Bronze is consistently clean for Silver to consume.

    Leading underscores are preserved — metadata columns (_snapshot_date, _ingest_ts,
    _source_file, _rescued_data) must keep their prefix so Silver can reference them.
    """
    _bad = re.compile(r"[ ,;{}\(\)\n\t=]+")
    renamed = []
    for c in df.columns:
        new_name = _bad.sub("_", c)
        # Only strip trailing underscores from substitutions; never strip leading ones
        # (metadata cols like _snapshot_date start with _ intentionally).
        if not c.startswith("_"):
            new_name = new_name.strip("_")
        renamed.append(F.col(f"`{c}`").alias(new_name))
    return df.select(renamed)

# retailer slug -> sub-path under SOURCE_ROOT (matches land_to_s3.py layout)
RETAILERS = ["aldi", "coles", "iga", "woolworths"]


def _bronze_table(retailer: str):
    """Define one Auto Loader streaming table for a retailer."""

    @dlt.table(
        name=f"raw_{retailer}",
        comment=f"Raw {retailer} price snapshots, ingested incrementally via Auto Loader.",
        table_properties={"quality": "bronze", "pipelines.reset.allowed": "true"},
    )
    def _ingest(retailer=retailer):  # bind loop var
        path = f"{SOURCE_ROOT}/{retailer}/"
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("header", "true")
            .option("rescuedDataColumn", "_rescued_data")
            .load(path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn(
                "_snapshot_date",
                F.to_date(
                    F.regexp_extract(F.col("_metadata.file_path"),
                                     r"date=(\d{4}-\d{2}-\d{2})", 1)
                ),
            )
            .transform(_sanitize_columns)   # ← strip spaces/special chars from headers
        )

    return _ingest


# Materialise a streaming table per retailer.
for _r in RETAILERS:
    _bronze_table(_r)
