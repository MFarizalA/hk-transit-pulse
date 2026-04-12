/* @bruin
name: marts.mart_trip_trajectories
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
  - staging.stg_stops
  - staging.stg_trips
  - staging.stg_routes
@bruin */

-- Trip trajectories for Kepler.gl animation
-- Each row = one stop visit with lat/lon + time offset in seconds
-- Kepler.gl Trip layer expects: trip_id, timestamp (numeric), latitude, longitude

SELECT
    st.trip_id,
    r.route_short_name,
    r.route_long_name,
    st.stop_sequence,
    s.stop_name,
    s.latitude,
    s.longitude,
    st.departure_time,
    -- Convert HH:MM:SS to Unix timestamp (ms) for Kepler.gl Trip layer
    -- Base: 2024-01-01 00:00:00 UTC = 1704067200 seconds
    (1704067200 +
      CAST(SPLIT(st.departure_time, ':')[OFFSET(0)] AS INT64) * 3600
      + CAST(SPLIT(st.departure_time, ':')[OFFSET(1)] AS INT64) * 60
      + CAST(SPLIT(st.departure_time, ':')[OFFSET(2)] AS INT64)
    ) * 1000 AS departure_timestamp_ms
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times` st
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_stops` s
    ON st.stop_id = s.stop_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t
    ON st.trip_id = t.trip_id
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_routes` r
    ON t.route_id = r.route_id
WHERE s.latitude IS NOT NULL
  AND s.longitude IS NOT NULL
  AND st.departure_time IS NOT NULL
  -- Filter valid HH:MM:SS format only
  AND REGEXP_CONTAINS(st.departure_time, r'^\d+:\d{2}:\d{2}$')
ORDER BY st.trip_id, st.stop_sequence
