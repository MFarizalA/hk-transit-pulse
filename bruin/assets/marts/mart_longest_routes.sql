/* @bruin
name: marts.mart_longest_routes
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_trips
  - staging.stg_routes
@bruin */

-- Longest routes by number of unique stops
SELECT
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT st.stop_id) AS unique_stops,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    MAX(st.stop_sequence) AS max_stop_sequence
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r ON t.route_id = r.route_id
GROUP BY r.route_short_name, r.route_long_name, r.route_type
ORDER BY unique_stops DESC
LIMIT 20
