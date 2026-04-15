/* @bruin
name: marts.mart_service_frequency
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_trips
  - staging.stg_routes
@bruin */

-- Service frequency: trips per hour per route
SELECT
    r.route_short_name,
    r.route_type,
    CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) AS hour_of_day,
    COUNT(DISTINCT st.trip_id) AS trips_per_hour
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t
    ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r
    ON t.route_id = r.route_id
WHERE st.departure_time IS NOT NULL
  AND REGEXP_CONTAINS(st.departure_time, r'^\d+:\d{2}:\d{2}$')
  AND CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) BETWEEN 0 AND 23
GROUP BY r.route_short_name, r.route_type, hour_of_day
ORDER BY route_short_name, hour_of_day
