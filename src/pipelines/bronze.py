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

import dlt
from pyspark.sql import functions as F

# Resolve the raw-zone root from pipeline config; fall back to a UC Volume so the
# pipeline still runs if external S3 isn't available on Databricks Free Edition.
SOURCE_ROOT = spark.conf.get(  # noqa: F821  (spark provided by DLT runtime)
    "source.root", "/Volumes/supermarket/bronze/landing/raw"
)

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
        )

    return _ingest


# Materialise a streaming table per retailer.
for _r in RETAILERS:
    _bronze_table(_r)
