# Output Schema

## Hotspot Grid (`hotspot-pipeline/output/`)

Partitioned by `city`. Available as Parquet and CSV.

| Column                   | Type    | Description                                 |
|--------------------------|---------|---------------------------------------------|
| `zone_id`                | String  | Geo zone identifier                         |
| `neighbourhood`          | String  | Neighbourhood name                          |
| `city`                   | String  | City                                        |
| `state`                  | String  | State / region                              |
| `zone_type`              | String  | residential / commercial / mixed / etc.     |
| `study_id`               | String  | Study this zone belongs to                  |
| `campaign_id`            | String  | Campaign identifier                         |
| `device_density_per_km2` | Double  | Devices per km² (from geo provider)         |
| `zone_area_km2`          | Double  | Zone area in km²                            |
| `order_hour`             | Integer | Hour of day (0–23)                          |
| `day_of_week`            | Integer | Day of week (1=Sun … 7=Sat)                 |
| `order_count`            | Long    | Orders in this zone × hour × day            |
| `avg_order_value_usd`    | Double  | Average order value (USD)                   |
| `distinct_customers`     | Long    | Unique customers                            |
| `distinct_restaurants`   | Long    | Unique restaurants                          |
| `top_cuisine`            | String  | Most common cuisine type                    |
| `city_rank`              | Integer | Zone rank within city by order_count        |

## Summary Report (`hotspot-pipeline/summary/`)

One CSV file per run window. Includes underserved zone flags.

| Column                   | Type    | Description                                      |
|--------------------------|---------|--------------------------------------------------|
| `zone_id`                | String  | Geo zone identifier                              |
| `neighbourhood`          | String  | Neighbourhood name                               |
| `city`                   | String  | City                                             |
| `state`                  | String  | State / region                                   |
| `zone_type`              | String  | Zone classification                              |
| `study_id`               | String  | Study identifier                                 |
| `campaign_id`            | String  | Campaign identifier                              |
| `device_density_per_km2` | Double  | Device density from geo provider                 |
| `zone_area_km2`          | Double  | Zone area                                        |
| `total_orders`           | Long    | Total orders across window                       |
| `avg_order_value_usd`    | Double  | Average order value                              |
| `total_customers`        | Long    | Distinct customers across window                 |
| `orders_per_device_ratio`| Double  | Orders ÷ estimated devices (coverage metric)     |
| `peak_hour`              | Integer | Peak delivery hour                               |
| `top_cuisine`            | String  | Most ordered cuisine                             |
| `is_underserved`         | Boolean | True if high device density + low order coverage |
| `window_start`           | String  | Pipeline run start date                          |
| `window_end`             | String  | Pipeline run end date                            |
