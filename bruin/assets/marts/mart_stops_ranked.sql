/* @bruin
name: marts.mart_stops_ranked
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_stops
@bruin */

SELECT
    s.stop_id,
    s.stop_name,
    s.latitude,
    s.longitude,
    COUNT(st.trip_id) AS total_departures
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_stops` s
    ON st.stop_id = s.stop_id
GROUP BY s.stop_id, s.stop_name, s.latitude, s.longitude
ORDER BY total_departures DESC
