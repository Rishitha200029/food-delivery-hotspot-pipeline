# Architecture

## Overview

The Food Delivery Hotspot Pipeline is a privacy-safe, multi-party analytics
pipeline built on Apache Airflow, PySpark, Amazon EMR, and Amazon Clean Rooms.

## Data Flow

```
Party A (Food Delivery Platform)          Party B (Geo Provider)
  order_events table                        geo_zone_devices table
  - order_id                                - zone_id
  - neighbourhood / city                    - neighbourhood / city
  - order_timestamp                         - device_density_per_km2
  - order_value_usd                         - zone_type
  - cuisine_type                            - avg_dwell_time_min
         │                                         │
         └──────────────┬──────────────────────────┘
                        │
              Amazon Clean Rooms
              Protected Query
              (aggregated output only)
                        │
              S3 Clean Rooms Output
                        │
              PySpark on EMR
              ┌──────────────────────┐
              │ Stage 3: enrich_zones│  ← zone_registry.csv
              │ Stage 4: build_grid  │  ← interim order events
              │ Stage 5: write_results│
              │ Stage 6: write_summary│
              └──────────────────────┘
                        │
              ┌─────────┴──────────┐
              │                    │
         Hotspot Grid          Summary Report
         (Parquet + CSV)       (underserved flags)
```

## EMR Cluster Spec

| Property          | Value        |
|-------------------|--------------|
| Release           | emr-6.3.0    |
| Master node       | m5.4xlarge × 1 |
| Core nodes        | m5.4xlarge × 4 |
| Spark executor mem| 12g          |
| Spark driver mem  | 8g           |
| Shuffle partitions| 200          |

## Amazon Clean Rooms Setup

1. Create a collaboration in the AWS Console
2. Party A configures analysis rules on `order_events` (aggregation only)
3. Party B configures analysis rules on `geo_zone_devices` (aggregation only)
4. Both parties accept the collaboration
5. Set minimum group size = 100 (suppress small groups for privacy)
6. Configure output to S3

## Airflow DAG

```
setup_window
    → provision_cluster
    → stage_script
    → launch_job
    → job_monitor
    → teardown_cluster  (trigger_rule=all_done — always runs)
    → notify_success / notify_failure
```
