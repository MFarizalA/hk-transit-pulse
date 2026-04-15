/* @bruin
name: marts.mart_peak_hour_analysis
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_stop_times
@bruin */

SELECT
    CAST(SPLIT(departure_time, ':')[OFFSET(0)] AS INT64) AS hour_of_day,
    COUNT(trip_id) AS total_trips
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_stop_times`
WHERE departure_time IS NOT NULL
GROUP BY hour_of_day
ORDER BY total_trips DESC
