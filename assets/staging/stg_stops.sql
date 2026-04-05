/* @bruin
name: staging.stg_stops
type: bq.sql
materialization:
  type: table
depends:
  - raw.gtfs_static_hk-transport
@bruin */

SELECT
    CAST(stop_id AS STRING)       AS stop_id,
    CAST(stop_name AS STRING)     AS stop_name,
    CAST(stop_lat AS FLOAT64)     AS latitude,
    CAST(stop_lon AS FLOAT64)     AS longitude
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_stops`
WHERE stop_id IS NOT NULL
