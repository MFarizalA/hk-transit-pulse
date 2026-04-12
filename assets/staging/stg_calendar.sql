/* @bruin
name: staging.stg_calendar
type: bq.sql
materialization:
  type: table
depends:
  - raw.gtfs_static_hk-transport
@bruin */

SELECT
    CAST(service_id AS STRING)  AS service_id,
    CAST(monday AS INT64)       AS monday,
    CAST(tuesday AS INT64)      AS tuesday,
    CAST(wednesday AS INT64)    AS wednesday,
    CAST(thursday AS INT64)     AS thursday,
    CAST(friday AS INT64)       AS friday,
    CAST(saturday AS INT64)     AS saturday,
    CAST(sunday AS INT64)       AS sunday,
    CAST(start_date AS STRING)  AS start_date,
    CAST(end_date AS STRING)    AS end_date,
    -- Derived flags
    (monday + tuesday + wednesday + thursday + friday) AS weekday_count,
    (saturday + sunday) AS weekend_count
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_calendar`
WHERE service_id IS NOT NULL
