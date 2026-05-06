"""
test_hotspot_pipeline.py
-------------------------
Unit tests for the Food Delivery Hotspot Analytics Pipeline.

Run with:
    pytest tests/ -v
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType
)

# ── Spark fixture ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("hotspot-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

# ── date_range ─────────────────────────────────────────────────────────────────

class TestDateRange:
    def test_single_day(self):
        from scripts.hotspot_spark import date_range
        assert date_range("2025-01-01", "2025-01-01") == ["2025-01-01"]

    def test_seven_days(self):
        from scripts.hotspot_spark import date_range
        result = date_range("2025-01-01", "2025-01-07")
        assert len(result) == 7
        assert result[0] == "2025-01-01"
        assert result[-1] == "2025-01-07"

    def test_ascending_order(self):
        from scripts.hotspot_spark import date_range
        result = date_range("2025-06-01", "2025-06-05")
        assert result == sorted(result)

# ── interim_output_exists ──────────────────────────────────────────────────────

class TestInterimOutputExists:
    @patch("scripts.hotspot_spark.boto3")
    def test_true_when_objects_exist(self, mock_boto3):
        from scripts.hotspot_spark import interim_output_exists
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"KeyCount": 5}
        assert interim_output_exists("2025-01-01") is True

    @patch("scripts.hotspot_spark.boto3")
    def test_false_when_empty(self, mock_boto3):
        from scripts.hotspot_spark import interim_output_exists
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {"KeyCount": 0}
        assert interim_output_exists("2025-01-01") is False

# ── Order deduplication ────────────────────────────────────────────────────────

class TestOrderDeduplication:
    def test_dedup_on_order_id_and_date(self, spark):
        schema = StructType([
            StructField("order_id",        StringType(), True),
            StructField("event_date",      StringType(), True),
            StructField("customer_id",     StringType(), True),
            StructField("order_value_usd", DoubleType(), True),
        ])
        rows = [
            ("ORD_001", "2025-01-01", "CUST_1", 15.0),
            ("ORD_001", "2025-01-01", "CUST_1", 15.0),  # duplicate
            ("ORD_002", "2025-01-01", "CUST_2", 22.5),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = df.dropDuplicates(["order_id", "event_date"])
        assert result.count() == 2

    def test_zero_value_orders_excluded(self, spark):
        schema = StructType([
            StructField("order_id",        StringType(), True),
            StructField("order_value_usd", DoubleType(), True),
        ])
        rows = [("ORD_001", 0.0), ("ORD_002", 12.5), ("ORD_003", -1.0)]
        df = spark.createDataFrame(rows, schema=schema)
        result = df.filter(F.col("order_value_usd") > 0)
        assert result.count() == 1

# ── Hotspot grid aggregation ───────────────────────────────────────────────────

class TestHotspotGrid:
    def test_order_count_aggregation(self, spark):
        schema = StructType([
            StructField("zone_id",         StringType(),  True),
            StructField("neighbourhood",   StringType(),  True),
            StructField("city",            StringType(),  True),
            StructField("order_id",        StringType(),  True),
            StructField("order_value_usd", DoubleType(),  True),
            StructField("customer_id",     StringType(),  True),
            StructField("restaurant_id",   StringType(),  True),
            StructField("cuisine_type",    StringType(),  True),
            StructField("order_hour",      IntegerType(), True),
            StructField("day_of_week",     IntegerType(), True),
        ])
        rows = [
            ("Z001", "Manhattan", "New York", "O1", 15.0, "C1", "R1", "Pizza",   12, 2),
            ("Z001", "Manhattan", "New York", "O2", 20.0, "C2", "R1", "Pizza",   12, 2),
            ("Z001", "Manhattan", "New York", "O3", 10.0, "C1", "R2", "Burgers", 12, 2),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        result = (
            df.groupBy("zone_id", "neighbourhood", "city", "order_hour", "day_of_week")
              .agg(
                  F.count("order_id").alias("order_count"),
                  F.round(F.avg("order_value_usd"), 2).alias("avg_order_value_usd"),
                  F.countDistinct("customer_id").alias("distinct_customers"),
              )
        )
        row = result.first()
        assert row["order_count"]         == 3
        assert row["distinct_customers"]  == 2
        assert abs(row["avg_order_value_usd"] - 15.0) < 0.01

# ── Underserved zone flagging ──────────────────────────────────────────────────

class TestUnderservedZones:
    def test_high_density_low_ratio_flagged(self, spark):
        schema = StructType([
            StructField("zone_id",                 StringType(), True),
            StructField("device_density_per_km2",  DoubleType(), True),
            StructField("orders_per_device_ratio", DoubleType(), True),
        ])
        rows = [
            ("Z001", 4500.0, 0.001),   # high density, low ratio → underserved
            ("Z002", 300.0,  0.150),   # low density, high ratio → not underserved
            ("Z003", 4800.0, 0.200),   # high density, high ratio → not underserved
        ]
        df = spark.createDataFrame(rows, schema=schema)

        density_threshold = 4000.0
        ratio_median      = 0.05

        result = df.withColumn(
            "is_underserved",
            F.when(
                (F.col("device_density_per_km2") >= density_threshold)
                & (F.col("orders_per_device_ratio") < ratio_median),
                True,
            ).otherwise(False),
        )

        underserved = result.filter(F.col("is_underserved")).collect()
        assert len(underserved) == 1
        assert underserved[0]["zone_id"] == "Z001"

# ── Synthetic data generator ───────────────────────────────────────────────────

class TestSyntheticDataGenerator:
    def test_jitter_stays_within_radius(self):
        from data_generator.generate_synthetic_data import jitter
        import math
        lat, lon = 40.7831, -73.9712
        for _ in range(100):
            jlat, jlon = jitter(lat, lon, radius=0.02)
            assert abs(jlat - lat) <= 0.02
            assert abs(jlon - lon) <= 0.02

    def test_random_order_value_positive(self):
        from data_generator.generate_synthetic_data import random_order_value
        for _ in range(100):
            val = random_order_value()
            assert val > 0

    def test_date_range_in_generator(self):
        from data_generator.generate_synthetic_data import CITIES
        assert "New York"    in CITIES
        assert "Los Angeles" in CITIES
        assert "Chicago"     in CITIES
        assert "Houston"     in CITIES
