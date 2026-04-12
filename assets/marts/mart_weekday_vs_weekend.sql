/* @bruin
name: marts.mart_weekday_vs_weekend
type: bq.sql
materialization:
  type: table
depends:
  - staging.stg_trips
  - staging.stg_calendar
@bruin */

-- Weekday vs weekend service comparison
SELECT
    CASE
        WHEN c.weekday_count > 0 AND c.weekend_count > 0 THEN 'Both'
        WHEN c.weekday_count > 0 THEN 'Weekday Only'
        WHEN c.weekend_count > 0 THEN 'Weekend Only'
        ELSE 'Unknown'
    END AS service_type,
    COUNT(DISTINCT t.trip_id) AS total_trips
FROM `project-e5d4de8a-49cc-439d-b6e.staging.stg_trips` t
JOIN `project-e5d4de8a-49cc-439d-b6e.staging.stg_calendar` c
    ON t.service_id = c.service_id
GROUP BY service_type
ORDER BY total_trips DESC
