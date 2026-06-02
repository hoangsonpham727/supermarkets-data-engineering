"""Silver layer — conform the four heterogeneous feeds into one model + dimensions.

Lakeflow Declarative Pipeline (DLT). Responsibilities:
  1. Map each retailer's native columns to a single conformed schema.
  2. Apply parsing rules (unit price, list strings, brand, surrogate SKU).
  3. Enforce data-quality expectations:
       - drop structurally-invalid rows (bad retailer / missing name / null price
         on an *available* item),
       - quarantine suspicious-but-usable rows (price/unit mismatch, out of range,
         unavailable-with-no-price) — kept, flagged out of the BI marts.
  4. Build conformed dimensions: dim_product (SCD2), dim_category, dim_retailer,
     dim_date.

The pure-Python parsing/DQ logic lives in ``src.common`` so it is unit-tested.
"""

import os
import sys

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

# --- make src.common importable inside the DLT runtime --------------------- #
# With Databricks Asset Bundles the repo is synced to workspace files; walk up
# from this file to the project root so `from src.common ...` resolves.
_here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
for _cand in (_here, os.path.dirname(_here), os.path.dirname(os.path.dirname(_here))):
    if os.path.isdir(os.path.join(_cand, "src", "common")) and _cand not in sys.path:
        sys.path.insert(0, _cand)

from src.common import parsers as P  # noqa: E402
from src.common import dq as DQ      # noqa: E402

# --------------------------------------------------------------------------- #
# UDF registration (thin wrappers over the tested pure-Python helpers)
# --------------------------------------------------------------------------- #
_unit_schema = StructType([
    StructField("value", DoubleType()),
    StructField("measure", StringType()),
])
_cat_schema = StructType([
    StructField("l1", StringType()),
    StructField("l2", StringType()),
])
_dq_schema = StructType([
    StructField("action", StringType()),
    StructField("reason", StringType()),
])

udf_unit = F.udf(lambda s: P.parse_unit_price(s), _unit_schema)
udf_norm_unit = F.udf(lambda s: P.normalize_unit_price(s), _unit_schema)
udf_first = F.udf(lambda s: P.first_from_list(s), StringType())
udf_sku = F.udf(P.make_surrogate_sku, StringType())
udf_brand = F.udf(P.extract_brand, StringType())
udf_avail = F.udf(P.normalize_availability, StringType())
udf_special = F.udf(lambda a, b: P.to_special_bool(a, b), BooleanType())
udf_price = F.udf(P.clean_price, DoubleType())
udf_cat = F.udf(lambda m, s: list(P.canonical_category(m, s)), _cat_schema)
udf_classify = F.udf(
    lambda r, n, p, a, u: list(DQ.classify_row(r, n, p, a, u)), _dq_schema
)

CONFORMED_COLS = [
    "retailer", "source_sku", "product_name", "brand",
    "price_aud", "previous_price_aud",
    "unit_price_value", "unit_price_measure",
    "norm_unit_price", "norm_unit_measure",
    "package_size_raw", "on_special", "availability", "ratings",
    "primary_supplier", "raw_main_category", "raw_sub_category",
    "canonical_l1", "canonical_l2", "product_url",
    "snapshot_date", "_source_file", "_ingest_ts",
]


def _col(df, name):
    """Column if present in df, else typed NULL — lets one builder serve all feeds."""
    return F.col(f"`{name}`") if name in df.columns else F.lit(None)


def _conform(df, *, retailer, name, sku=None, brand=None, price=None,
             prev_price=None, unit_price=None, pkg=None, on_special_cols=(),
             avail=None, ratings=None, supplier_list=None,
             main_cat=None, sub_cat=None, url=None):
    """Project a retailer's raw bronze df onto the conformed schema."""
    name_c = _col(df, name)
    url_c = _col(df, url) if url else F.lit(None)

    src_sku = _col(df, sku) if sku else udf_sku(F.lit(retailer), name_c, url_c)
    brand_c = _col(df, brand) if brand else udf_brand(name_c)
    unit = udf_unit(_col(df, unit_price)) if unit_price else F.struct(
        F.lit(None).cast("double").alias("value"), F.lit(None).alias("measure"))
    norm = udf_norm_unit(_col(df, unit_price)) if unit_price else unit

    special_args = [(_col(df, c)) for c in on_special_cols] or [F.lit(None), F.lit(None)]
    while len(special_args) < 2:
        special_args.append(F.lit(None))
    cat = udf_cat(_col(df, main_cat) if main_cat else F.lit(None),
                  _col(df, sub_cat) if sub_cat else F.lit(None))

    return df.select(
        F.lit(retailer).alias("retailer"),
        src_sku.alias("source_sku"),
        name_c.alias("product_name"),
        brand_c.alias("brand"),
        udf_price(_col(df, price)).alias("price_aud") if price
            else F.lit(None).cast("double").alias("price_aud"),
        udf_price(_col(df, prev_price)).alias("previous_price_aud") if prev_price
            else F.lit(None).cast("double").alias("previous_price_aud"),
        unit["value"].alias("unit_price_value"),
        unit["measure"].alias("unit_price_measure"),
        norm["value"].alias("norm_unit_price"),
        norm["measure"].alias("norm_unit_measure"),
        (_col(df, pkg) if pkg else F.lit(None)).alias("package_size_raw"),
        udf_special(*special_args[:2]).alias("on_special"),
        udf_avail(_col(df, avail) if avail else F.lit(None)).alias("availability"),
        (_col(df, ratings) if ratings else F.lit(None)).cast("double").alias("ratings"),
        (udf_first(_col(df, supplier_list)) if supplier_list else F.lit(None))
            .alias("primary_supplier"),
        (_col(df, main_cat) if main_cat else F.lit(None)).alias("raw_main_category"),
        (_col(df, sub_cat) if sub_cat else F.lit(None)).alias("raw_sub_category"),
        cat["l1"].alias("canonical_l1"),
        cat["l2"].alias("canonical_l2"),
        url_c.alias("product_url"),
        F.col("_snapshot_date").alias("snapshot_date"),
        F.col("_source_file"),
        F.col("_ingest_ts"),
    )


# --------------------------------------------------------------------------- #
# Per-retailer conformed streaming views
# --------------------------------------------------------------------------- #
@dlt.view(name="norm_aldi")
def norm_aldi():
    # Bronze sanitises column names: spaces → underscores.
    # "Unit Price" -> "Unit_Price", "Main Category" -> "Main_Category", etc.
    return _conform(
        dlt.read_stream("raw_aldi"), retailer="Aldi",
        name="Product", price="Price", unit_price="Unit_Price",
        main_cat="Main_Category", sub_cat="Sub_Category", url="Product_Page",
    )


@dlt.view(name="norm_coles")
def norm_coles():
    return _conform(
        dlt.read_stream("raw_coles"), retailer="Coles",
        name="Product Name", sku="SKU", brand="Brand", price="Current Price",
        prev_price="Previous Price", unit_price="Price per unit", pkg="Unit Size",
        on_special_cols=("On Special",), avail="Availability",
        supplier_list="Suppliers", main_cat="Category", url="URL",
    )


@dlt.view(name="norm_iga")
def norm_iga():
    return _conform(
        dlt.read_stream("raw_iga"), retailer="IGA",
        name="Product Name", sku="SKU", price="Price",
        unit_price="Price per unit", main_cat="Main Category",
        sub_cat="Sub Category", url="Product URL",
    )


@dlt.view(name="norm_woolworths")
def norm_woolworths():
    df = dlt.read_stream("raw_woolworths")
    # WOW 'Department' is a stringified list -> reduce to a single clean category.
    df = df.withColumn("_dept", udf_first(_col(df, "Department")))
    return _conform(
        df, retailer="Woolworths",
        name="Product Name", sku="SKU", brand="Brand", price="Price",
        unit_price="Price per unit", pkg="Package Size",
        on_special_cols=("Specials",), avail="Availability", ratings="Ratings",
        main_cat="_dept", url="Product URL",
    )


# --------------------------------------------------------------------------- #
# Conformed fact with DQ expectations + quarantine flag
# --------------------------------------------------------------------------- #
@dlt.table(
    name="fact_price",
    comment="Conformed daily price fact across all retailers (grain: retailer x product x date).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_retailer", "retailer IN ('Aldi','Coles','IGA','Woolworths')")
@dlt.expect_or_drop("has_product_name", "product_name IS NOT NULL AND product_name <> ''")
@dlt.expect("price_present_or_unavailable",
            "price_aud IS NOT NULL OR availability = 'unavailable'")
def fact_price():
    unioned = (
        dlt.read_stream("norm_aldi")
        .unionByName(dlt.read_stream("norm_coles"))
        .unionByName(dlt.read_stream("norm_iga"))
        .unionByName(dlt.read_stream("norm_woolworths"))
    )
    cls = udf_classify(
        F.col("retailer"), F.col("product_name"), F.col("price_aud"),
        F.col("availability"), F.col("unit_price_value"),
    )
    return (
        unioned
        .withColumn("_dq", cls)
        .withColumn("dq_action", F.col("_dq.action"))
        .withColumn("dq_reason", F.col("_dq.reason"))
        # drop only the structurally-unusable rows; keep quarantined ones flagged
        .filter(F.col("dq_action") != "drop")
        .withColumn("is_quarantined", F.col("dq_action") == "quarantine")
        .withColumn("product_sk", F.md5(F.concat_ws("|", "retailer", "source_sku")))
        .withColumn("category_sk", F.md5(F.concat_ws("|", "canonical_l1", "canonical_l2")))
        .drop("_dq")
    )


@dlt.table(name="fact_price_quarantine",
           comment="Rows flagged by DQ rules — kept for review, excluded from BI marts.")
def fact_price_quarantine():
    return dlt.read("fact_price").filter(F.col("is_quarantined"))


# --------------------------------------------------------------------------- #
# Dimensions
# --------------------------------------------------------------------------- #
# dim_product as Slowly Changing Dimension Type 2, keyed by (retailer, source_sku),
# sequenced by snapshot_date, tracking descriptive attributes.
dlt.create_streaming_table(
    name="dim_product",
    comment="SCD2 product dimension — tracks attribute changes over time.",
)
dlt.apply_changes(
    target="dim_product",
    source="fact_price",
    keys=["product_sk"],
    sequence_by=F.col("snapshot_date"),
    stored_as_scd_type=2,
    track_history_column_list=["product_name", "brand", "package_size_raw",
                               "canonical_l1", "canonical_l2"],
    except_column_list=["price_aud", "previous_price_aud", "on_special",
                        "availability", "is_quarantined", "dq_action", "dq_reason",
                        "_ingest_ts", "_source_file"],
)


@dlt.table(name="dim_category", comment="Canonical two-level category hierarchy.")
def dim_category():
    return (
        dlt.read("fact_price")
        .select("category_sk", "canonical_l1", "canonical_l2")
        .dropDuplicates(["category_sk"])
    )


@dlt.table(name="dim_retailer", comment="Static retailer reference dimension.")
def dim_retailer():
    rows = [
        ("Aldi", "ALDI Australia", "Discounter"),
        ("Coles", "Coles Group", "Full-service"),
        ("IGA", "Metcash (IGA)", "Independent"),
        ("Woolworths", "Woolworths Group", "Full-service"),
    ]
    schema = StructType([
        StructField("retailer", StringType()),
        StructField("parent_company", StringType()),
        StructField("retailer_type", StringType()),
    ])
    return (spark.createDataFrame(rows, schema)  # noqa: F821
            .withColumn("retailer_sk", F.md5(F.col("retailer"))))


@dlt.table(name="dim_date", comment="Generated calendar dimension.")
def dim_date():
    base = (dlt.read("fact_price")
            .select(F.min("snapshot_date").alias("mn"),
                    F.max("snapshot_date").alias("mx"))
            .collect()[0])
    return (
        spark.sql(  # noqa: F821
            f"SELECT explode(sequence(to_date('{base['mn']}'), "
            f"to_date('{base['mx']}'), interval 1 day)) AS date_day"
        )
        .withColumn("date_sk", F.date_format("date_day", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date_day"))
        .withColumn("month", F.month("date_day"))
        .withColumn("day", F.dayofmonth("date_day"))
        .withColumn("day_of_week", F.dayofweek("date_day"))
        .withColumn("is_weekend", F.dayofweek("date_day").isin(1, 7))
    )
