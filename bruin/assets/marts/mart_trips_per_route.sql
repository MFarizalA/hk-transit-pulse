/* @bruin
name: marts.mart_trips_per_route
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_trips
  - staging.stg_routes
@bruin */

SELECT
    r.route_short_name,
    r.route_long_name,
    COUNT(t.trip_id) AS total_trips
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r
    ON t.route_id = r.route_id
GROUP BY r.route_short_name, r.route_long_name
ORDER BY total_trips DESC
