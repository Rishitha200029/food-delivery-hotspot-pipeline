# 🍕 Food Delivery Hotspot Analytics Pipeline

A production-grade **Apache Airflow + PySpark + Amazon Clean Rooms** pipeline
that performs privacy-safe collaboration between two parties to identify food
delivery hotspots, peak demand windows, and underserved geo zones.

---

## Business Problem

A **food delivery platform** (Party A) wants to enrich its order data with
neighbourhood-level geo attributes from a **telecom/geo provider** (Party B).
Neither party can share raw data with the other.

**Amazon Clean Rooms** acts as the privacy layer — only aggregated, anonymised
match results leave the collaboration. Raw data never crosses the boundary.

```
┌─────────────────────────┐         ┌─────────────────────────┐
│   Party A               │         │   Party B               │
│   Food Delivery Platform│         │   Geo Provider          │
│                         │         │                         │
│  order_id               │         │  device_id              │
│  pickup_lat / lon       │◀──────▶│  geo_zone               │
│  delivery_lat / lon     │  Clean  │  neighbourhood          │
│  order_timestamp        │  Rooms  │  city / state           │
│  order_value            │         │  zone_type              │
└────────────┬────────────┘         └────────────┬────────────┘
             │                                    │
             └──────────────┬─────────────────────┘
                            │
                  ┌─────────▼──────────┐
                  │  Protected Query   │
                  │  (aggregated only) │
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │   PySpark (EMR)    │
                  │   Enrichment &     │
                  │   Aggregation      │
                  └─────────┬──────────┘
                            │
                  ┌─────────▼──────────┐
                  │   Hotspot Report   │
                  │   + Heatmap Data   │
                  └────────────────────┘
```

---

## What the Pipeline Produces

1. **Hotspot Report** — top delivery zones ranked by order volume, avg value,
   and peak hour, grouped by neighbourhood and city
2. **Hourly Demand Grid** — order counts by geo zone × hour-of-day × day-of-week
3. **Underserved Zone Report** — zones with high device density but low delivery
   coverage (growth opportunity signal)
4. **Summary CSV** — one row per geo zone with all KPIs, ready for dashboarding

---

## Repository Structure

```
food-delivery-hotspot-pipeline/
├── dags/
│   └── hotspot_analytics_dag.py         # Airflow DAG (weekly)
├── scripts/
│   ├── hotspot_spark.py                 # PySpark pipeline (6 stages)
│   └── cleanrooms_query.sql             # Protected query template
├── shell/
│   ├── provision_cluster.sh             # EMR cluster creation
│   └── launch_job.sh                    # Spark job submission
├── config/
│   ├── params.example.json              # All config (safe to commit)
│   └── zone_registry.example.csv        # Geo zone metadata
├── data_generator/
│   └── generate_synthetic_data.py       # Generate fake order + geo data
├── docs/
│   ├── architecture.md                  # Deep-dive architecture
│   └── output_schema.md                 # Output column definitions
├── tests/
│   └── test_hotspot_pipeline.py         # pytest unit tests
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Prerequisites

| Requirement     | Version   |
|-----------------|-----------|
| Python          | 3.8+      |
| Apache Airflow  | 2.7+      |
| PySpark         | 3.1.3     |
| AWS CLI         | v2        |
| boto3           | 1.26+     |

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/your-username/food-delivery-hotspot-pipeline.git
cd food-delivery-hotspot-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate synthetic data (no real data needed!)

```bash
python data_generator/generate_synthetic_data.py \
    --output-dir ./sample_data \
    --days 7 \
    --orders-per-day 5000
```

This creates realistic synthetic order events and geo zone data you can use
to run the full pipeline locally or upload to S3 for EMR testing.

### 3. Configure

```bash
cp config/params.example.json config/params.json
# Edit config/params.json with your S3 buckets, EMR subnet, etc.
```

### 4. Upload to S3

```bash
aws s3 cp scripts/hotspot_spark.py       s3://your-metadata-bucket/hotspot-pipeline/scripts/
aws s3 cp scripts/cleanrooms_query.sql   s3://your-metadata-bucket/hotspot-pipeline/scripts/
aws s3 cp config/zone_registry.csv       s3://your-metadata-bucket/hotspot-pipeline/config/
```

### 5. Deploy DAG

```bash
cp dags/hotspot_analytics_dag.py /path/to/airflow/dags/
```

---

## Pipeline Stages

| Stage | Function              | Description                                              |
|-------|-----------------------|----------------------------------------------------------|
| 1     | `load_order_events`   | Ingest daily order ORC files, deduplicate, write interim |
| 2     | `run_cleanrooms_query`| Trigger protected query in Amazon Clean Rooms            |
| 3     | `enrich_zones`        | Join Clean Rooms output with geo zone registry           |
| 4     | `build_hotspot_grid`  | Compute hourly demand grid per geo zone                  |
| 5     | `write_results`       | Write hotspot report (Parquet + CSV)                     |
| 6     | `write_summary`       | Aggregate KPIs, flag underserved zones                   |

---

## Amazon Clean Rooms Setup

1. Create a **collaboration** in the AWS Console between Party A and Party B
2. Configure **analysis rules** on each table (aggregation only — no row-level output)
3. Add the protected query template from `scripts/cleanrooms_query.sql`
4. Set output location to your S3 results bucket
5. Update `config/params.json` with your `collaboration_id` and `membership_id`

See [`docs/architecture.md`](docs/architecture.md) for the full Clean Rooms
setup walkthrough.

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Key Design Decisions

- **Clean Rooms for privacy** — raw order locations and device data never
  leave their respective owners; only aggregated zone-level counts are shared.
- **Synthetic data generator** — anyone can clone and run this pipeline
  without needing real data, making it a true open-source project.
- **Idempotent Stage 1** — daily partitions are skipped if interim output
  already exists; safe to re-run and backfill.
- **Exponential back-off polling** — Clean Rooms query status is polled
  with 30s → 60s → 120s → 300s ceiling to avoid API throttling.
- **Underserved zone detection** — zones where geo provider device density
  exceeds a threshold but delivery coverage is below average are flagged
  as growth opportunities.

---

## License

MIT — free to use, fork, and build on.
