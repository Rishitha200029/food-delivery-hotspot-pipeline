-- cleanrooms_query.sql
-- ----------------------
-- Protected query for the Food Delivery Hotspot Pipeline.
-- Run inside Amazon Clean Rooms collaboration between:
--   Party A — food delivery platform (contributes: order_events)
--   Party B — geo provider           (contributes: geo_zone_devices)
--
-- Analysis rules enforced by Clean Rooms:
--   - Aggregation only (no row-level output)
--   - Minimum group size: 100 records
--   - No direct join key output
--
-- Output: zone-level order counts and avg value, safe to share with both parties.

SELECT
    gz.zone_id,
    gz.neighbourhood,
    gz.city,
    gz.state,
    gz.zone_type,
    gz.device_density_per_km2,
    gz.avg_dwell_time_min,
    gz.zone_area_km2,

    -- Order volume metrics (from Party A)
    COUNT(oe.order_id)                          AS total_orders,
    COUNT(DISTINCT oe.customer_id)              AS distinct_customers,
    COUNT(DISTINCT oe.restaurant_id)            AS distinct_restaurants,
    ROUND(AVG(oe.order_value_usd), 2)           AS avg_order_value_usd,
    ROUND(SUM(oe.order_value_usd), 2)           AS total_order_value_usd,

    -- Peak hour (mode of order hour)
    MODE(EXTRACT(HOUR FROM oe.order_timestamp)) AS peak_hour,

    -- Cuisine breakdown
    MODE(oe.cuisine_type)                       AS top_cuisine,

    -- Coverage ratio: orders per device (delivery demand vs device presence)
    ROUND(
        COUNT(oe.order_id)::FLOAT
        / NULLIF(gz.device_density_per_km2 * gz.zone_area_km2, 0),
        4
    )                                           AS orders_per_device_ratio,

    oe.event_date

FROM
    party_a.order_events     oe
    INNER JOIN party_b.geo_zone_devices gz
        ON  oe.neighbourhood = gz.neighbourhood
        AND oe.city          = gz.city

WHERE
    oe.event_date BETWEEN :start_date AND :end_date

GROUP BY
    gz.zone_id,
    gz.neighbourhood,
    gz.city,
    gz.state,
    gz.zone_type,
    gz.device_density_per_km2,
    gz.avg_dwell_time_min,
    gz.zone_area_km2,
    oe.event_date

-- Clean Rooms enforces minimum group size — groups with < 100 orders are suppressed
HAVING
    COUNT(oe.order_id) >= 100

ORDER BY
    total_orders DESC,
    gz.city,
    gz.neighbourhood;
