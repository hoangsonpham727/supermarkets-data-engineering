"""Gold layer — BI-ready star schema + pre-aggregated marts.

Lakeflow Declarative Pipeline (DLT). Consumes Silver (clean, conformed) and
produces the tables the AI/BI dashboards read. Quarantined rows are excluded
from all price marts (we analyse only trustworthy prices).
"""

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F


def _clean_fact():
    """Silver fact with quarantined rows removed — the basis for all marts."""
    return dlt.read("fact_price").filter(~F.col("is_quarantined"))


# --------------------------------------------------------------------------- #
# Central fact: one row per retailer x product x day, with day-over-day change
# --------------------------------------------------------------------------- #
@dlt.table(
    name="fact_price_daily",
    comment="Star-schema price fact with day-over-day price change; FKs to dims.",
    table_properties={"quality": "gold"},
)
def fact_price_daily():
    w = Window.partitionBy("product_sk").orderBy("snapshot_date")
    return (
        _clean_fact()
        .withColumn("prev_day_price", F.lag("price_aud").over(w))
        .withColumn(
            "price_change_pct",
            F.when(
                F.col("prev_day_price").isNotNull() & (F.col("prev_day_price") != 0),
                F.round((F.col("price_aud") - F.col("prev_day_price"))
                        / F.col("prev_day_price") * 100, 2),
            ),
        )
        .withColumn("date_sk", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
        .withColumn("retailer_sk", F.md5("retailer"))
        .select(
            "product_sk", "category_sk", "retailer_sk", "date_sk",
            "retailer", "source_sku", "product_name", "brand",
            "price_aud", "previous_price_aud", "prev_day_price", "price_change_pct",
            "unit_price_value", "unit_price_measure",
            "norm_unit_price", "norm_unit_measure",
            "on_special", "availability", "ratings",
            "canonical_l1", "canonical_l2", "product_url", "snapshot_date",
        )
    )


# --------------------------------------------------------------------------- #
# Mart 1: cross-retailer price comparison (normalised unit price by category)
# --------------------------------------------------------------------------- #
@dlt.table(name="mart_price_comparison",
           comment="Cheapest retailer per canonical category by normalised unit price.")
def mart_price_comparison():
    base = (
        dlt.read("fact_price_daily")
        .filter(F.col("norm_unit_price").isNotNull())
        .groupBy("snapshot_date", "canonical_l1", "canonical_l2", "norm_unit_measure",
                 "retailer")
        .agg(F.round(F.avg("norm_unit_price"), 2).alias("avg_norm_unit_price"),
             F.count("*").alias("n_products"))
    )
    w = Window.partitionBy("snapshot_date", "canonical_l1", "canonical_l2",
                           "norm_unit_measure").orderBy("avg_norm_unit_price")
    return (base
            .withColumn("price_rank", F.rank().over(w))
            .withColumn("is_cheapest", F.col("price_rank") == 1))


# --------------------------------------------------------------------------- #
# Mart 2: specials penetration (% of range on special)
# --------------------------------------------------------------------------- #
@dlt.table(name="mart_specials_penetration",
           comment="Share of products on special by retailer x category x day.")
def mart_specials_penetration():
    return (
        dlt.read("fact_price_daily")
        .groupBy("snapshot_date", "retailer", "canonical_l1")
        .agg(F.count("*").alias("n_products"),
             F.sum(F.col("on_special").cast("int")).alias("n_on_special"))
        .withColumn("specials_pct",
                    F.round(F.col("n_on_special") / F.col("n_products") * 100, 2))
    )


# --------------------------------------------------------------------------- #
# Mart 3: category price index (a 'basket' index per retailer over time)
# --------------------------------------------------------------------------- #
@dlt.table(name="mart_category_price_index",
           comment="Avg normalised unit price by category x retailer x day (basket index).")
def mart_category_price_index():
    daily = (
        dlt.read("fact_price_daily")
        .filter(F.col("norm_unit_price").isNotNull())
        .groupBy("snapshot_date", "retailer", "canonical_l1", "norm_unit_measure")
        .agg(F.round(F.avg("norm_unit_price"), 2).alias("avg_norm_unit_price"),
             F.count("*").alias("n_products"))
    )
    # index = price relative to each series' first observed day (=100)
    w = Window.partitionBy("retailer", "canonical_l1", "norm_unit_measure") \
              .orderBy("snapshot_date")
    return (daily
            .withColumn("base_price", F.first("avg_norm_unit_price").over(w))
            .withColumn("price_index",
                        F.round(F.col("avg_norm_unit_price") / F.col("base_price") * 100, 1)))


# --------------------------------------------------------------------------- #
# Mart 4: availability rate
# --------------------------------------------------------------------------- #
@dlt.table(name="mart_availability",
           comment="Availability rate by retailer x day.")
def mart_availability():
    return (
        dlt.read("fact_price")  # includes unavailable rows by design
        .groupBy("snapshot_date", "retailer")
        .agg(F.count("*").alias("n_listings"),
             F.sum((F.col("availability") == "available").cast("int")).alias("n_available"),
             F.sum((F.col("availability") == "unavailable").cast("int")).alias("n_unavailable"))
        .withColumn("availability_rate",
                    F.round(F.col("n_available") / F.col("n_listings") * 100, 2))
    )


# --------------------------------------------------------------------------- #
# Mart 5: data-quality scorecard (feeds the DQ dashboard page)
# --------------------------------------------------------------------------- #
@dlt.table(name="mart_dq_scorecard",
           comment="Per-source row counts, quarantine rate and null-price rate.")
def mart_dq_scorecard():
    return (
        dlt.read("fact_price")
        .groupBy("snapshot_date", "retailer")
        .agg(F.count("*").alias("n_rows"),
             F.sum(F.col("is_quarantined").cast("int")).alias("n_quarantined"),
             F.sum(F.col("price_aud").isNull().cast("int")).alias("n_null_price"))
        .withColumn("quarantine_pct",
                    F.round(F.col("n_quarantined") / F.col("n_rows") * 100, 2))
        .withColumn("null_price_pct",
                    F.round(F.col("n_null_price") / F.col("n_rows") * 100, 2))
    )
