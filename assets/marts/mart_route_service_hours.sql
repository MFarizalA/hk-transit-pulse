/* @bruin
name: marts.mart_route_service_hours
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_trips
  - staging.stg_routes
@bruin */

-- First and last service time per route
SELECT
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    MIN(st.departure_time) AS first_departure,
    MAX(st.departure_time) AS last_departure,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    COUNT(DISTINCT st.stop_id) AS total_stops
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t
    ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r
    ON t.route_id = r.route_id
WHERE st.departure_time IS NOT NULL
  AND REGEXP_CONTAINS(st.departure_time, r'^\d+:\d{2}:\d{2}$')
GROUP BY r.route_short_name, r.route_long_name, r.route_type
ORDER BY total_trips DESC
