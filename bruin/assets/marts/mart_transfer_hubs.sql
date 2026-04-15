/* @bruin
name: marts.mart_transfer_hubs
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_trips
  - staging.stg_routes
  - staging.stg_stops
@bruin */

-- Transfer hubs: stops served by multiple routes
SELECT
    s.stop_id,
    s.stop_name,
    s.latitude,
    s.longitude,
    COUNT(DISTINCT r.route_id) AS route_count,
    COUNT(DISTINCT r.route_type) AS transport_modes,
    STRING_AGG(DISTINCT r.route_short_name ORDER BY r.route_short_name LIMIT 10) AS routes_serving
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r ON t.route_id = r.route_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_stops` s ON st.stop_id = s.stop_id
WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
GROUP BY s.stop_id, s.stop_name, s.latitude, s.longitude
HAVING route_count >= 3
ORDER BY route_count DESC
