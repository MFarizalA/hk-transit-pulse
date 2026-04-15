/* @bruin
name: staging.stg_routes
type: bq.sql
materialization:
  type: table
depends:
  - raw.gtfs_static_hk-transport
@bruin */

SELECT
    CAST(route_id AS STRING)         AS route_id,
    CAST(route_short_name AS STRING) AS route_short_name,
    CAST(route_long_name AS STRING)  AS route_long_name,
    CAST(route_type AS INT64)        AS route_type
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_routes`
WHERE route_id IS NOT NULL
