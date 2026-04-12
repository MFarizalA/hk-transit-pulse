# Architecture

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  Sources                                                        │
│  ┌──────────────────────────┐  ┌──────────────────────────┐    │
│  │ HK Transport GTFS Static │  │ MTR Open Data CSVs       │    │
│  │ data.gov.hk              │  │ opendata.mtr.com.hk      │    │
│  └──────────┬───────────────┘  └───────────┬──────────────┘    │
└─────────────┼──────────────────────────────┼───────────────────┘
              │                              │
              ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Ingestion (Bruin Python Assets)                                │
│  ingest_gtfs_static.py          ingest_mtr_csv.py              │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Data Lake — Google Cloud Storage                               │
│  hk-transit-pulse-raw/                                         │
│  ├── gtfs_static/hk-transport/  (routes, stops, trips, …)     │
│  └── mtr_static/                (lines, bus stops, fares, …)  │
└─────────────────────────┬───────────────────────────────────────┘
                          │  BQ Load Jobs (WRITE_TRUNCATE)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  BigQuery — raw dataset                                         │
│  gtfs_routes, gtfs_stops, gtfs_trips, gtfs_stop_times,        │
│  gtfs_calendar, mtr_lines_stations, mtr_bus_stops,            │
│  mtr_fares, mtr_light_rail_stops                               │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Bruin SQL staging assets
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  BigQuery — staging dataset                                     │
│  stg_stops, stg_routes, stg_trips, stg_stop_times,            │
│  stg_calendar                                                  │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Bruin SQL mart assets
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  BigQuery — marts dataset                                       │
│  mart_stops_ranked, mart_trips_per_route,                      │
│  mart_peak_hour_analysis, mart_route_service_hours,            │
│  mart_service_frequency, mart_transfer_hubs,                   │
│  mart_weekday_vs_weekend, mart_longest_routes,                 │
│  mart_early_night_routes, mart_trip_trajectories               │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Streamlit Dashboard (dashboard/app.py)                         │
│  Network Analytics · MTR Live · Streaming Analytics · About    │
└─────────────────────────────────────────────────────────────────┘
```

## Layer Responsibilities

| Layer | Location | Responsibility |
|---|---|---|
| Ingestion | `assets/ingestion/` | Fetch raw data → GCS → BigQuery raw |
| Staging | `assets/staging/` | Cast types, rename columns, filter nulls |
| Marts | `assets/marts/` | Aggregate and shape data for dashboard queries |
| Dashboard | `dashboard/app.py` | Query marts directly via BigQuery client |

## Scheduling

Pipeline runs daily at **06:00 HKT** (UTC+8), defined as cron `0 22 * * *` UTC in `pipeline.yml`.

Execution order is determined by Bruin from `depends:` declarations in asset headers — no manual ordering needed.

## Infrastructure

Provisioned via OpenTofu (`terraform/`):

- **GCS bucket:** `hk-transit-pulse-raw` (raw data lake)
- **BigQuery datasets:** `raw`, `staging`, `marts` (all in US region)
- **Service account:** pipeline runner with Storage Object Admin + BigQuery Data Editor roles
