/* @bruin
name: marts.mart_early_night_routes
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_trips
  - staging.stg_routes
@bruin */

-- Early bird routes (first departure before 06:00)
-- Night owl routes (last departure after 23:00)
SELECT
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    MIN(st.departure_time) AS first_departure,
    MAX(st.departure_time) AS last_departure,
    CAST(SPLIT(MIN(st.departure_time), ':')[OFFSET(0)] AS INT64) AS first_hour,
    CAST(SPLIT(MAX(st.departure_time), ':')[OFFSET(0)] AS INT64) AS last_hour,
    CASE
        WHEN CAST(SPLIT(MIN(st.departure_time), ':')[OFFSET(0)] AS INT64) < 6 THEN TRUE
        ELSE FALSE
    END AS is_early_bird,
    CASE
        WHEN CAST(SPLIT(MAX(st.departure_time), ':')[OFFSET(0)] AS INT64) >= 23 THEN TRUE
        ELSE FALSE
    END AS is_night_owl
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r ON t.route_id = r.route_id
WHERE st.departure_time IS NOT NULL
  AND REGEXP_CONTAINS(st.departure_time, r'^\d+:\d{2}:\d{2}$')
GROUP BY r.route_short_name, r.route_long_name, r.route_type
HAVING is_early_bird OR is_night_owl
ORDER BY first_hour ASC
