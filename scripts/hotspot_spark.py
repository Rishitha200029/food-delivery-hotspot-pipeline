"""
hotspot_spark.py
-----------------
PySpark pipeline for the Food Delivery Hotspot Analytics Pipeline.

Stages:
    1. load_order_events    — Ingest daily order CSVs, deduplicate, write interim
    2. run_cleanrooms_query — Trigger Amazon Clean Rooms protected query
    3. enrich_zones         — Join Clean Rooms output with geo zone registry
    4. build_hotspot_grid   — Compute hourly demand grid per zone
    5. write_results        — Write hotspot report (Parquet + CSV)
    6. write_summary        — Aggregate KPIs, flag underserved zones

Usage:
    spark-submit hotspot_spark.py \
        --start-date 2025-01-01 \
        --end-date   2025-01-07 \
        --metadata-bucket your-metadata-bucket \
        --collaboration-id <clean-rooms-collab-id> \
        --membership-id   <clean-rooms-membership-id>
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta

import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.window import Window

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── S3 / config constants ──────────────────────────────────────────────────────

SRC_BUCKET          = "your-source-bucket"
SRC_PREFIX          = "order-events"

GEO_STORE_BUCKET    = "your-geo-store-bucket"
GEO_STORE_PREFIX    = "geo-zones"

INTERIM_BUCKET      = "your-interim-bucket"
INTERIM_PREFIX      = "hotspot-pipeline/interim"

OUTPUT_BUCKET       = "your-output-bucket"
OUTPUT_PREFIX       = "hotspot-pipeline/output"

SUMMARY_BUCKET      = "your-output-bucket"
SUMMARY_PREFIX      = "hotspot-pipeline/summary"

METADATA_BUCKET     = "your-metadata-bucket"
ZONE_REGISTRY_PATH  = f"s3://{METADATA_BUCKET}/hotspot-pipeline/config/zone_registry.csv"
CLEANROOMS_QUERY    = f"s3://{METADATA_BUCKET}/hotspot-pipeline/scripts/cleanrooms_query.sql"
CLEANROOMS_OUTPUT   = f"s3://{OUTPUT_BUCKET}/hotspot-pipeline/cleanrooms-output"

# Underserved zone threshold:
# A zone is flagged if device density is above this percentile
# but orders_per_device_ratio is below the median
UNDERSERVED_DENSITY_PERCENTILE = 0.75

# ── Spark session ──────────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FoodDeliveryHotspotPipeline")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

# ── Date utilities ─────────────────────────────────────────────────────────────

def date_range(start_date: str, end_date: str) -> list[str]:
    """Return list of YYYY-MM-DD strings from start_date to end_date inclusive."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    return [
        str(start + timedelta(days=i))
        for i in range((end - start).days + 1)
    ]

# ── S3 utilities ───────────────────────────────────────────────────────────────

def s3_path_exists(bucket: str, prefix: str) -> bool:
    """Return True if at least one object exists under s3://bucket/prefix."""
    s3   = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


def interim_output_exists(date: str) -> bool:
    prefix = f"{INTERIM_PREFIX}/event_date={date}/"
    return s3_path_exists(INTERIM_BUCKET, prefix)


def poll_query_results(
    cleanrooms_client,
    membership_id: str,
    query_id: str,
    timeout_seconds: int = 3600,
) -> str:
    """
    Poll Amazon Clean Rooms until the protected query completes.
    Uses exponential back-off: 30s → 60s → 120s → 300s ceiling.
    Returns the S3 output path on success.
    """
    wait      = 30
    max_wait  = 300
    elapsed   = 0
    terminal  = {"SUCCESS", "FAILED", "CANCELLED"}

    while elapsed < timeout_seconds:
        response = cleanrooms_client.get_protected_query(
            membershipIdentifier=membership_id,
            protectedQueryIdentifier=query_id,
        )
        status = response["protectedQuery"]["status"]
        log.info("[Clean Rooms] Query %s — status: %s (%ds elapsed)", query_id, status, elapsed)

        if status in terminal:
            if status != "SUCCESS":
                raise RuntimeError(f"Clean Rooms query {query_id} ended with status: {status}")
            result = response["protectedQuery"]["resultConfiguration"]["outputConfiguration"]
            return result["s3"]["resultFormat"]

        time.sleep(wait)
        elapsed += wait
        wait     = min(wait * 2, max_wait)

    raise TimeoutError(f"Clean Rooms query {query_id} did not finish within {timeout_seconds}s.")

# ── Stage 1: Load order events ─────────────────────────────────────────────────

def load_order_events(spark: SparkSession, date: str) -> None:
    """
    Read raw order CSV files for a single date, deduplicate on
    (order_id, event_date), and write interim Parquet partitioned by event_date.

    Skips the date if interim output already exists (idempotent).
    """
    if interim_output_exists(date):
        log.info("[Stage 1] Interim output already exists for %s — skipping.", date)
        return

    log.info("[Stage 1] Loading order events for %s ...", date)

    src_path = f"s3://{SRC_BUCKET}/{SRC_PREFIX}/orders_{date}.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(src_path)
    )

    df_clean = (
        df
        .select(
            F.col("order_id"),
            F.col("event_date"),
            F.to_timestamp("order_timestamp").alias("order_timestamp"),
            F.col("customer_id"),
            F.col("restaurant_id"),
            F.col("cuisine_type"),
            F.col("pickup_lat").cast(DoubleType()),
            F.col("pickup_lon").cast(DoubleType()),
            F.col("delivery_lat").cast(DoubleType()),
            F.col("delivery_lon").cast(DoubleType()),
            F.col("order_value_usd").cast(DoubleType()),
            F.col("city"),
            F.col("neighbourhood"),
        )
        .dropDuplicates(["order_id", "event_date"])
        .filter(F.col("order_value_usd") > 0)   # drop corrupt rows
        .withColumn("order_hour", F.hour("order_timestamp"))
        .withColumn("day_of_week", F.dayofweek("order_timestamp"))
    )

    out_path = f"s3://{INTERIM_BUCKET}/{INTERIM_PREFIX}"
    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(out_path)
    )
    log.info("[Stage 1] Interim written for %s → %s", date, out_path)

# ── Stage 2: Run Clean Rooms protected query ───────────────────────────────────

def run_cleanrooms_query(
    collaboration_id: str,
    membership_id: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Trigger a protected query in Amazon Clean Rooms.

    The query joins Party A's order_events with Party B's geo_zone_devices
    and returns aggregated zone-level metrics — no raw rows are exposed.

    Returns the S3 path where Clean Rooms wrote the output.
    """
    log.info("[Stage 2] Triggering Clean Rooms protected query (%s → %s) ...", start_date, end_date)

    client = boto3.client("cleanrooms", region_name="us-east-1")

    # Read the SQL template from S3
    s3      = boto3.client("s3")
    key     = CLEANROOMS_QUERY.replace(f"s3://{METADATA_BUCKET}/", "")
    sql_obj = s3.get_object(Bucket=METADATA_BUCKET, Key=key)
    sql     = sql_obj["Body"].read().decode("utf-8")

    # Substitute date parameters
    sql = sql.replace(":start_date", f"'{start_date}'")
    sql = sql.replace(":end_date",   f"'{end_date}'")

    response = client.start_protected_query(
        type="SQL",
        membershipIdentifier=membership_id,
        sqlParameters={"queryString": sql},
        resultConfiguration={
            "outputConfiguration": {
                "s3": {
                    "resultFormat": "CSV",
                    "bucket": OUTPUT_BUCKET,
                    "keyPrefix": f"hotspot-pipeline/cleanrooms-output/{start_date}_to_{end_date}/",
                }
            }
        },
    )

    query_id = response["protectedQuery"]["id"]
    log.info("[Stage 2] Protected query started: %s", query_id)

    # Poll until complete
    output_path = poll_query_results(client, membership_id, query_id)
    log.info("[Stage 2] Clean Rooms query complete. Output at: %s", output_path)
    return output_path

# ── Stage 3: Enrich with zone registry ────────────────────────────────────────

def enrich_zones(spark: SparkSession, cleanrooms_output_path: str) -> DataFrame:
    """
    Read the Clean Rooms output CSV and join with the zone registry to
    attach study and campaign metadata.

    Returns an enriched DataFrame ready for hotspot grid computation.
    """
    log.info("[Stage 3] Enriching Clean Rooms output with zone registry ...")

    df_cr = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(cleanrooms_output_path)
    )

    df_registry = (
        spark.read
        .option("header", "true")
        .csv(ZONE_REGISTRY_PATH)
        .select("zone_id", "study_id", "campaign_id", "target_city",
                "min_order_threshold", "ext1", "ext2")
    )

    df_enriched = (
        df_cr
        .join(df_registry, on="zone_id", how="left")
        .filter(F.col("total_orders") >= F.col("min_order_threshold").cast(IntegerType()))
    )

    log.info("[Stage 3] Zone enrichment complete — %d zones.", df_enriched.count())
    return df_enriched

# ── Stage 4: Build hotspot grid ────────────────────────────────────────────────

def build_hotspot_grid(spark: SparkSession, df_enriched: DataFrame, start_date: str, end_date: str) -> DataFrame:
    """
    Join enriched zone data back with interim order events to build
    a fine-grained hourly demand grid per zone.

    Output columns per zone × hour × day_of_week:
        - order_count
        - avg_order_value
        - distinct_customers
        - distinct_restaurants
        - top_cuisine
    """
    log.info("[Stage 4] Building hourly hotspot demand grid ...")

    interim_path = f"s3://{INTERIM_BUCKET}/{INTERIM_PREFIX}"
    df_orders = (
        spark.read.parquet(interim_path)
        .filter(F.col("event_date").between(start_date, end_date))
    )

    # Join orders with enriched zone data on neighbourhood + city
    df_grid = (
        df_orders
        .join(
            df_enriched.select(
                "zone_id", "neighbourhood", "city", "state", "zone_type",
                "device_density_per_km2", "zone_area_km2",
                "study_id", "campaign_id", "orders_per_device_ratio",
            ),
            on=["neighbourhood", "city"],
            how="inner",
        )
        .groupBy(
            "zone_id", "neighbourhood", "city", "state", "zone_type",
            "study_id", "campaign_id",
            "device_density_per_km2", "zone_area_km2",
            "order_hour", "day_of_week",
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.avg("order_value_usd"), 2).alias("avg_order_value_usd"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.countDistinct("restaurant_id").alias("distinct_restaurants"),
            F.first("cuisine_type").alias("top_cuisine"),
        )
    )

    # Rank zones by order_count within each city
    window_city = Window.partitionBy("city").orderBy(F.desc("order_count"))
    df_grid = df_grid.withColumn("city_rank", F.rank().over(window_city))

    log.info("[Stage 4] Hotspot grid built.")
    return df_grid

# ── Stage 5: Write results ─────────────────────────────────────────────────────

def write_results(df_grid: DataFrame, start_date: str, end_date: str) -> None:
    """
    Write the hotspot demand grid in two formats:
      1. Snappy Parquet — for analytics and ad-hoc querying
      2. CSV           — for dashboard ingestion / sharing

    Partitioned by city.
    """
    log.info("[Stage 5] Writing hotspot grid results ...")

    parquet_path = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/parquet/{start_date}_to_{end_date}"
    csv_path     = f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}/csv/{start_date}_to_{end_date}"

    (
        df_grid
        .write
        .mode("overwrite")
        .partitionBy("city")
        .parquet(parquet_path)
    )
    log.info("[Stage 5] Parquet written → %s", parquet_path)

    (
        df_grid
        .coalesce(4)
        .write
        .mode("overwrite")
        .option("header", "true")
        .partitionBy("city")
        .csv(csv_path)
    )
    log.info("[Stage 5] CSV written → %s", csv_path)

# ── Stage 6: Write summary + flag underserved zones ───────────────────────────

def write_summary(spark: SparkSession, df_enriched: DataFrame, start_date: str, end_date: str) -> None:
    """
    Aggregate KPIs at zone level across the full window.
    Flag underserved zones: high device density but low delivery coverage.

    Underserved = device_density_per_km2 in top 25%
                  AND orders_per_device_ratio below median.
    """
    log.info("[Stage 6] Writing summary + flagging underserved zones ...")

    # Compute percentile thresholds
    density_threshold = df_enriched.approxQuantile(
        "device_density_per_km2", [UNDERSERVED_DENSITY_PERCENTILE], 0.01
    )[0]

    ratio_median = df_enriched.approxQuantile(
        "orders_per_device_ratio", [0.5], 0.01
    )[0]

    df_summary = (
        df_enriched
        .groupBy("zone_id", "neighbourhood", "city", "state",
                 "zone_type", "study_id", "campaign_id",
                 "device_density_per_km2", "zone_area_km2")
        .agg(
            F.sum("total_orders").alias("total_orders"),
            F.round(F.avg("avg_order_value_usd"), 2).alias("avg_order_value_usd"),
            F.sum("distinct_customers").alias("total_customers"),
            F.round(F.avg("orders_per_device_ratio"), 4).alias("orders_per_device_ratio"),
            F.first("peak_hour").alias("peak_hour"),
            F.first("top_cuisine").alias("top_cuisine"),
        )
        .withColumn(
            "is_underserved",
            F.when(
                (F.col("device_density_per_km2") >= density_threshold)
                & (F.col("orders_per_device_ratio") < ratio_median),
                True,
            ).otherwise(False),
        )
        .withColumn("window_start", F.lit(start_date))
        .withColumn("window_end",   F.lit(end_date))
        .orderBy(F.desc("total_orders"))
    )

    summary_path = f"s3://{SUMMARY_BUCKET}/{SUMMARY_PREFIX}/{start_date}_to_{end_date}"
    (
        df_summary
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(summary_path)
    )

    underserved = df_summary.filter(F.col("is_underserved")).count()
    total       = df_summary.count()
    log.info("[Stage 6] Summary written → %s", summary_path)
    log.info("[Stage 6] %d / %d zones flagged as underserved.", underserved, total)

# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Food Delivery Hotspot Analytics Pipeline")
    parser.add_argument("--start-date",        required=True)
    parser.add_argument("--end-date",          required=True)
    parser.add_argument("--metadata-bucket",   default=METADATA_BUCKET)
    parser.add_argument("--collaboration-id",  required=True, help="Amazon Clean Rooms collaboration ID")
    parser.add_argument("--membership-id",     required=True, help="Amazon Clean Rooms membership ID")
    return parser.parse_args()


def main():
    args  = parse_args()
    spark = get_spark()

    log.info("=" * 60)
    log.info("Food Delivery Hotspot Analytics Pipeline")
    log.info("Window : %s → %s", args.start_date, args.end_date)
    log.info("=" * 60)

    # Stage 1 — ingest and interim-stage each day (idempotent)
    for date in date_range(args.start_date, args.end_date):
        load_order_events(spark, date)

    # Stage 2 — Amazon Clean Rooms protected query
    cleanrooms_output = run_cleanrooms_query(
        args.collaboration_id,
        args.membership_id,
        args.start_date,
        args.end_date,
    )

    # Stage 3 — enrich with zone registry
    df_enriched = enrich_zones(spark, cleanrooms_output)

    # Stage 4 — build hourly hotspot grid
    df_grid = build_hotspot_grid(spark, df_enriched, args.start_date, args.end_date)

    # Cache before two writes
    df_grid.cache()
    df_enriched.cache()

    # Stage 5 — write hotspot grid (Parquet + CSV)
    write_results(df_grid, args.start_date, args.end_date)

    # Stage 6 — summary + underserved zone flagging
    write_summary(spark, df_enriched, args.start_date, args.end_date)

    df_grid.unpersist()
    df_enriched.unpersist()
    spark.stop()
    log.info("Pipeline complete. 🍕")


if __name__ == "__main__":
    main()
