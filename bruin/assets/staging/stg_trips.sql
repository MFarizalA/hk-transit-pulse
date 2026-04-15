/* @bruin
name: staging.stg_trips
type: bq.sql
materialization:
  type: table
depends:
  - raw.gtfs_static_hk-transport
@bruin */

SELECT
    CAST(trip_id AS STRING)      AS trip_id,
    CAST(route_id AS STRING)     AS route_id,
    CAST(service_id AS STRING)   AS service_id
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_trips`
WHERE trip_id IS NOT NULL
