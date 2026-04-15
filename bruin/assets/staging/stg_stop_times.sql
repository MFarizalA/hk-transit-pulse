/* @bruin
name: staging.stg_stop_times
type: bq.sql
materialization:
  type: table
depends:
  - raw.gtfs_static_hk-transport
@bruin */

SELECT
    CAST(trip_id AS STRING)          AS trip_id,
    CAST(stop_id AS STRING)          AS stop_id,
    CAST(stop_sequence AS INT64)     AS stop_sequence,
    CAST(arrival_time AS STRING)     AS arrival_time,
    CAST(departure_time AS STRING)   AS departure_time
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_stop_times`
WHERE trip_id IS NOT NULL
  AND stop_id IS NOT NULL
