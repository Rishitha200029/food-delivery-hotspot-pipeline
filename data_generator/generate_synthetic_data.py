"""
generate_synthetic_data.py
---------------------------
Generates realistic synthetic data for the Food Delivery Hotspot Pipeline.

Produces two datasets:
  1. Order events (Party A — food delivery platform)
  2. Geo zone device density (Party B — geo/telecom provider)

No real data is used. All locations, orders, and identifiers are fake.

Usage:
    python generate_synthetic_data.py \
        --output-dir ./sample_data \
        --days 7 \
        --orders-per-day 5000
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta

# ── Synthetic city / neighbourhood data ───────────────────────────────────────

CITIES = {
    "New York": {
        "state": "NY",
        "neighbourhoods": [
            ("Manhattan",    40.7831, -73.9712),
            ("Brooklyn",     40.6782, -73.9442),
            ("Queens",       40.7282, -73.7949),
            ("Bronx",        40.8448, -73.8648),
            ("Staten Island",40.5795, -74.1502),
        ],
    },
    "Los Angeles": {
        "state": "CA",
        "neighbourhoods": [
            ("Downtown LA",  34.0407, -118.2468),
            ("Hollywood",    34.0928, -118.3287),
            ("Santa Monica", 34.0195, -118.4912),
            ("Venice",       33.9850, -118.4695),
            ("Koreatown",    34.0584, -118.3006),
        ],
    },
    "Chicago": {
        "state": "IL",
        "neighbourhoods": [
            ("The Loop",     41.8827, -87.6233),
            ("Wicker Park",  41.9085, -87.6789),
            ("Lincoln Park", 41.9214, -87.6513),
            ("Hyde Park",    41.7943, -87.5907),
            ("Pilsen",       41.8566, -87.6638),
        ],
    },
    "Houston": {
        "state": "TX",
        "neighbourhoods": [
            ("Downtown",     29.7604, -95.3698),
            ("Midtown",      29.7385, -95.3777),
            ("Montrose",     29.7452, -95.3908),
            ("The Heights",  29.7996, -95.4002),
            ("EaDo",         29.7489, -95.3398),
        ],
    },
}

CUISINE_TYPES = [
    "Pizza", "Sushi", "Burgers", "Indian", "Mexican",
    "Thai", "Chinese", "Italian", "Sandwiches", "Salads",
]

ZONE_TYPES = ["residential", "commercial", "mixed", "university", "transit_hub"]


# ── Random helpers ─────────────────────────────────────────────────────────────

def jitter(lat: float, lon: float, radius: float = 0.02) -> tuple[float, float]:
    """Add small random offset to a coordinate."""
    return (
        round(lat + random.uniform(-radius, radius), 6),
        round(lon + random.uniform(-radius, radius), 6),
    )


def random_timestamp(date_str: str) -> str:
    """Generate a random timestamp within a given date, weighted toward meal times."""
    base = datetime.strptime(date_str, "%Y-%m-%d")
    # Weight toward lunch (11-14) and dinner (17-21)
    meal_windows = [(11, 14, 0.35), (17, 21, 0.45), (0, 23, 0.20)]
    roll = random.random()
    cumulative = 0.0
    for start_h, end_h, weight in meal_windows:
        cumulative += weight
        if roll <= cumulative:
            hour = random.randint(start_h, end_h)
            break
    else:
        hour = random.randint(0, 23)

    minute  = random.randint(0, 59)
    second  = random.randint(0, 59)
    ts      = base.replace(hour=hour, minute=minute, second=second)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def random_order_value() -> float:
    """Generate a realistic order value (USD)."""
    # Most orders $8–$40, occasional large orders
    if random.random() < 0.05:
        return round(random.uniform(50, 120), 2)
    return round(random.uniform(8, 40), 2)


# ── Generator functions ────────────────────────────────────────────────────────

def generate_order_events(
    output_path: str,
    date_str: str,
    num_orders: int,
) -> None:
    """
    Generate synthetic order events for Party A (food delivery platform).

    Schema:
        order_id, event_date, order_timestamp, customer_id,
        restaurant_id, cuisine_type, pickup_lat, pickup_lon,
        delivery_lat, delivery_lon, order_value_usd, city, neighbourhood
    """
    os.makedirs(output_path, exist_ok=True)
    filename = os.path.join(output_path, f"orders_{date_str}.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "event_date", "order_timestamp",
            "customer_id", "restaurant_id", "cuisine_type",
            "pickup_lat", "pickup_lon",
            "delivery_lat", "delivery_lon",
            "order_value_usd", "city", "neighbourhood",
        ])

        for _ in range(num_orders):
            city_name, city_data = random.choice(list(CITIES.items()))
            nbhd_name, nbhd_lat, nbhd_lon = random.choice(city_data["neighbourhoods"])

            pickup_lat,   pickup_lon   = jitter(nbhd_lat, nbhd_lon, 0.015)
            delivery_lat, delivery_lon = jitter(nbhd_lat, nbhd_lon, 0.025)

            writer.writerow([
                str(uuid.uuid4()),
                date_str,
                random_timestamp(date_str),
                f"CUST_{random.randint(10000, 99999)}",
                f"REST_{random.randint(1000,  9999)}",
                random.choice(CUISINE_TYPES),
                pickup_lat,   pickup_lon,
                delivery_lat, delivery_lon,
                random_order_value(),
                city_name,
                nbhd_name,
            ])

    print(f"[generator] Order events written → {filename}  ({num_orders} rows)")


def generate_geo_zone_data(output_path: str) -> None:
    """
    Generate synthetic geo zone device density data for Party B (geo provider).

    Schema:
        zone_id, neighbourhood, city, state, zone_type,
        centre_lat, centre_lon, device_density_per_km2,
        avg_dwell_time_min, zone_area_km2
    """
    os.makedirs(output_path, exist_ok=True)
    filename = os.path.join(output_path, "geo_zones.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "zone_id", "neighbourhood", "city", "state", "zone_type",
            "centre_lat", "centre_lon",
            "device_density_per_km2", "avg_dwell_time_min", "zone_area_km2",
        ])

        zone_id = 1
        for city_name, city_data in CITIES.items():
            for nbhd_name, nbhd_lat, nbhd_lon in city_data["neighbourhoods"]:
                zone_type        = random.choice(ZONE_TYPES)
                device_density   = round(random.uniform(200, 5000), 1)
                avg_dwell        = round(random.uniform(10, 90), 1)
                zone_area        = round(random.uniform(0.5, 8.0), 2)

                writer.writerow([
                    f"ZONE_{zone_id:04d}",
                    nbhd_name,
                    city_name,
                    city_data["state"],
                    zone_type,
                    round(nbhd_lat, 6),
                    round(nbhd_lon, 6),
                    device_density,
                    avg_dwell,
                    zone_area,
                ])
                zone_id += 1

    print(f"[generator] Geo zone data written → {filename}  ({zone_id - 1} zones)")


def generate_zone_registry(output_path: str) -> None:
    """
    Generate the zone_registry.csv used by the pipeline config.
    Maps zone_id to study / campaign metadata.
    """
    os.makedirs(output_path, exist_ok=True)
    filename = os.path.join(output_path, "zone_registry.csv")

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "zone_id", "study_id", "campaign_id", "target_city",
            "min_order_threshold", "ext1", "ext2",
        ])

        zone_id = 1
        for city_name, city_data in CITIES.items():
            for nbhd_name, _, _ in city_data["neighbourhoods"]:
                writer.writerow([
                    f"ZONE_{zone_id:04d}",
                    f"STUDY_{random.randint(100, 199)}",
                    f"CAMP_{random.randint(1000, 1099)}",
                    city_name,
                    random.randint(10, 50),
                    "",
                    "",
                ])
                zone_id += 1

    print(f"[generator] Zone registry written → {filename}")


# ── Main ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic data for hotspot pipeline")
    parser.add_argument("--output-dir",     default="./sample_data", help="Output directory")
    parser.add_argument("--days",           type=int, default=7,    help="Number of days to generate")
    parser.add_argument("--orders-per-day", type=int, default=5000, help="Orders per day")
    parser.add_argument("--seed",           type=int, default=42,   help="Random seed for reproducibility")
    return parser.parse_args()


def main():
    args = parse_args()
    random.seed(args.seed)

    print(f"\n{'=' * 50}")
    print("Food Delivery Hotspot — Synthetic Data Generator")
    print(f"{'=' * 50}")
    print(f"Output dir   : {args.output_dir}")
    print(f"Days         : {args.days}")
    print(f"Orders/day   : {args.orders_per_day}")
    print(f"Random seed  : {args.seed}")
    print(f"{'=' * 50}\n")

    orders_dir = os.path.join(args.output_dir, "order_events")
    geo_dir    = os.path.join(args.output_dir, "geo_zones")
    config_dir = os.path.join(args.output_dir, "config")

    # Generate order events for each day
    end_date   = datetime.utcnow().date() - timedelta(days=5)
    start_date = end_date - timedelta(days=args.days - 1)
    current    = start_date

    while current <= end_date:
        generate_order_events(orders_dir, str(current), args.orders_per_day)
        current += timedelta(days=1)

    # Generate geo zone data (static — generated once)
    generate_geo_zone_data(geo_dir)

    # Generate zone registry for pipeline config
    generate_zone_registry(config_dir)

    total_orders = args.days * args.orders_per_day
    print(f"\n✅ Done! Generated {total_orders:,} synthetic orders across {args.days} days.")
    print(f"   Upload to S3:")
    print(f"   aws s3 sync {orders_dir} s3://your-source-bucket/order-events/")
    print(f"   aws s3 sync {geo_dir}    s3://your-geo-store-bucket/geo-zones/")
    print(f"   aws s3 sync {config_dir} s3://your-metadata-bucket/hotspot-pipeline/config/\n")


if __name__ == "__main__":
    main()
