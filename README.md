# <img src="https://flagcdn.com/w40/hk.png" height="28"/> Hong Kong Transit Pulse — 香港交通脈搏

![GCP](https://img.shields.io/badge/Cloud-Google_Cloud_Platform-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/Warehouse-BigQuery-4285F4?style=flat-square&logo=googlebigquery&logoColor=white)
![GCS](https://img.shields.io/badge/Lake-Cloud_Storage-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![Bruin](https://img.shields.io/badge/Orchestration-Bruin-F97316?style=flat-square&logoColor=white)
![Redpanda](https://img.shields.io/badge/Streaming-Redpanda-E52B50?style=flat-square&logo=redpanda&logoColor=white)
![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![CloudRun](https://img.shields.io/badge/Deploy-Cloud_Run-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![GitHubActions](https://img.shields.io/badge/CI/CD-GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white)
![OpenTofu](https://img.shields.io/badge/IaC-OpenTofu-7B42BC?style=flat-square&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)

> **Data Engineering Zoomcamp 2026 — Capstone Project**

🚀 **[Live Dashboard → hk-transit-pulse-fclvz6lara-de.a.run.app](https://hk-transit-pulse-fclvz6lara-de.a.run.app)**

---

## Problem Statement

Hong Kong runs one of the world's most complex public transport networks — MTR heavy rail, Light Rail, over 700 bus routes, trams, and ferries — yet the data describing how these systems actually perform sits scattered across multiple government portals in raw GTFS files and CSV exports that are difficult for the public to interpret.

This project builds a structured batch pipeline to answer:

- Which stops and routes are most heavily used?
- How does service differ between weekdays and weekends?
- Which areas are underserved by public transport?
- When do first and last services run on each route?
- How do MTR and bus networks complement each other geographically?

---

## Overview

Hong Kong Transit Pulse is an end-to-end batch data engineering pipeline that ingests raw GTFS feeds and MTR open data, transforms them into analytics-ready models, and surfaces insights via an interactive Streamlit dashboard.

The pipeline runs daily, pulling from two open data sources — HK Transport (GTFS) and MTR Corporation — loading them into Google Cloud Storage, transforming through BigQuery layers (raw → staging → marts), and visualising in a 4-tab dashboard with a real-time streaming layer on top.

---

## Table of Contents

- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Data Sources](#data-sources)
- [Data Pipeline](#data-pipeline)
- [Insights & Visualizations](#insights--visualizations)
- [Steps to Reproduce](#steps-to-reproduce)
- [About](#about)

---

## Tech Stack

<p>
<img height="32" width="32" src="https://cdn.simpleicons.org/googlecloud" title="GCP"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/googlecloudstorage" title="GCS"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/googlebigquery" title="BigQuery"/>
<img height="32" width="32" src="DOCS/images/redpanda_logo.svg" title="Redpanda"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/streamlit" title="Streamlit"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/docker" title="Docker"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/githubactions" title="GitHub Actions"/>
<img height="32" width="32" src="DOCS/images/opentofu_logo.svg" title="OpenTofu"/>
<img height="32" width="32" src="DOCS/images/bruin_logo.svg" title="Bruin"/>
<img height="32" width="32" src="https://cdn.simpleicons.org/python" title="Python"/>
</p>

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Bruin | Pipeline orchestration + SQL transformations |
| Infrastructure | OpenTofu | Provision GCS, BigQuery, service accounts |
| Data Lake | Google Cloud Storage | Raw zone for GTFS and MTR files |
| Data Warehouse | BigQuery | raw → staging → marts layers |
| Streaming | Redpanda (Kafka-compatible) | Real-time MTR event stream via Redpanda Cloud |
| Visualization | Streamlit + pydeck | Interactive dashboard (4 tabs) |
| Containerization | Docker | Container images for dashboard, batch, and streaming |
| Deployment | Google Cloud Run | Dashboard (Service) + Batch + Streaming (Jobs) |
| CI/CD | GitHub Actions + WIF | Auto-deploy on push to main |
| Scheduling | Cloud Scheduler | Trigger streaming jobs (1 min) + batch job (daily) |
| Cloud | GCP Free Tier | Compute, storage, and warehouse |
| Language | Python 3.11 | Ingestion scripts and dashboard |

### Why These Technologies?

**BigQuery**
Chosen for its serverless, columnar architecture that handles large-scale analytical queries without managing infrastructure. We utilize it across three layers — `raw` (direct GCS loads), `staging` (cleaned and typed views), and `marts` (aggregated analytics tables) — plus a `streaming` dataset for real-time MTR events. BigQuery's pay-per-query model fits perfectly within GCP free tier for this project scale.

| Raw Layer | Staging Layer | Marts Layer | Streaming Layer |
|---|---|---|---|
| ![BQ Raw](DOCS/images/bq_raw.png) | ![BQ Staging](DOCS/images/bq_staging.png) | ![BQ Marts](DOCS/images/bq_marts.png) | ![BQ Streaming](DOCS/images/bq_streaming.png) |
| GTFS and MTR files loaded directly from GCS with schema autodetection | Cleaned, typed, and renamed columns ready for aggregation | 10 analytics-ready mart tables powering the dashboard | Real-time MTR schedule events streamed from Redpanda every minute |

**Redpanda**
A Kafka-compatible streaming platform with zero ZooKeeper dependency, lower latency, and a generous free cloud tier. We use Redpanda Cloud to bridge the gap between the MTR Schedule API (polled every minute) and BigQuery — the producer publishes train schedule events to `hk-mtr-schedule` topic, and the consumer drains them into BigQuery streaming inserts.

| Cluster Overview | Topics |
|---|---|
| ![Redpanda Overview](DOCS/images/redpanda_overview.png) | ![Redpanda Topics](DOCS/images/redpandas_topics.png) |
| Redpanda Cloud cluster hosting the HK Transit Pulse broker — showing throughput and partition health | Two topics in use: `hk-mtr-schedule` for train schedule events and `hk-bus-eta` for KMB bus ETAs |

**Streamlit**
Chosen for rapid dashboard development in pure Python — no frontend skills required. We leverage it for 4 interactive tabs: network analytics with pydeck maps, live MTR schedule polling, streaming event charts, and project background. Deployed as a Cloud Run Service for always-on public access.

| Network Analytics | MTR Live | Streaming Analytics |
|---|---|---|
| ![Network Analytics](DOCS/images/streamlit_network_analytics.png) | ![MTR Live](DOCS/images/streamlit_mtr_live.png) | ![Streaming Analytics](DOCS/images/streamlit_streaming_analytics.png) |
| Stop location maps, busiest routes, peak hour analysis, and coverage heatmaps filtered by transport type | Live MTR train schedules pulled from the real-time API with upbound/downbound breakdown and fare explorer | Real-time event volume, delay tracking, and top destinations from the Redpanda stream |

**OpenTofu**
The open-source Terraform fork used to provision all GCP infrastructure as code — GCS bucket, BigQuery datasets, service accounts, and IAM bindings. Ensures the entire infrastructure is reproducible from scratch with a single `tofu apply`.

**Bruin**
A modern data pipeline framework that unifies Python ingestion scripts and SQL transformation assets under one orchestrator. We use it to define the full batch pipeline — from GTFS download to mart creation — with dependency resolution, scheduling, and a single `bruin run` command. Deployed as a Cloud Run Job triggered daily by Cloud Scheduler.

---

## Architecture

```mermaid
flowchart TD
    GH["🔧 GitHub Actions\npush to main"]
    GH -->|deploy.yml| CR_DASH["☁️ Cloud Run Service\nhk-transit-pulse\nStreamlit Dashboard"]
    GH -->|batch.yml| CR_BATCH["☁️ Cloud Run Job\nbatch-job\nBruin Pipeline"]

    CS1["⏰ Cloud Scheduler\nevery 1 min"]
    CS2["⏰ Cloud Scheduler\nevery 1 min"]
    CS1 --> CR_PROD["☁️ Cloud Run Job\nproducer-job"]
    CS2 --> CR_CONS["☁️ Cloud Run Job\nconsumer-job"]

    A["🌐 HK Transport GTFS Static\ndata.gov.hk"]
    B["🚇 MTR Open Data CSVs\nopendata.mtr.com.hk"]
    MTR_API["🚇 MTR Schedule API\nrt.data.gov.hk"]

    CR_BATCH --> A
    CR_BATCH --> B
    A --> C["⚙️ Bruin Ingestion\ningest_gtfs_static.py"]
    B --> D["⚙️ Bruin Ingestion\ningest_mtr_csv.py"]

    C --> E["🪣 Google Cloud Storage\ngtfs_static/hk-transport/"]
    D --> F["🪣 Google Cloud Storage\nmtr_static/"]

    E -->|BQ Load Job| G["🗄️ BigQuery — raw\ngtfs_routes · gtfs_stops · gtfs_trips\ngtfs_stop_times · gtfs_calendar"]
    F -->|BQ Load Job| H["🗄️ BigQuery — raw\nmtr_lines_stations · mtr_bus_stops\nmtr_fares · mtr_light_rail_stops"]

    G --> I["🔧 Bruin Staging Assets\nstg_stops · stg_routes · stg_trips\nstg_stop_times · stg_calendar"]
    H --> I

    I --> J["📊 Bruin Mart Assets\nmart_stops_ranked · mart_trips_per_route\nmart_peak_hour_analysis · mart_route_service_hours\nmart_service_frequency · mart_transfer_hubs\nmart_weekday_vs_weekend · mart_longest_routes\nmart_early_night_routes · mart_trip_trajectories"]

    CR_PROD --> MTR_API
    MTR_API -->|events| RP["📨 Redpanda Cloud\nhk-mtr-schedule topic"]
    RP --> CR_CONS
    CR_CONS -->|streaming insert| BQ_STREAM["🗄️ BigQuery — streaming\nmtr_schedule_raw"]

    J --> CR_DASH
    BQ_STREAM --> CR_DASH

    style GH fill:#2088FF,color:#fff,stroke:#2088FF
    style CR_DASH fill:#4285F4,color:#fff,stroke:#4285F4
    style CR_BATCH fill:#4285F4,color:#fff,stroke:#4285F4
    style CR_PROD fill:#4285F4,color:#fff,stroke:#4285F4
    style CR_CONS fill:#4285F4,color:#fff,stroke:#4285F4
    style CS1 fill:#34A853,color:#fff,stroke:#34A853
    style CS2 fill:#34A853,color:#fff,stroke:#34A853
    style RP fill:#E52B50,color:#fff,stroke:#E52B50
    style A fill:#e8f4f8,stroke:#4285F4
    style B fill:#e8f4f8,stroke:#C8102E
    style C fill:#fff3e0,stroke:#F97316
    style D fill:#fff3e0,stroke:#F97316
    style E fill:#e3f2fd,stroke:#4285F4
    style F fill:#e3f2fd,stroke:#4285F4
    style G fill:#e8eaf6,stroke:#4285F4
    style H fill:#e8eaf6,stroke:#4285F4
    style I fill:#fff3e0,stroke:#F97316
    style J fill:#fff3e0,stroke:#F97316
    style BQ_STREAM fill:#e8eaf6,stroke:#4285F4
```

The data flow proceeds through the following steps:

1. **CI/CD** — Push to `main` triggers GitHub Actions. `deploy.yml` builds and deploys the dashboard to Cloud Run Service. `batch.yml` builds and deploys the batch job.
2. **Batch Ingestion** — Cloud Scheduler triggers `batch-job` daily at 06:00 HKT. Bruin Python assets fetch GTFS ZIP and MTR CSVs, uploading raw files to Google Cloud Storage.
3. **Raw Load** — BigQuery load jobs (WRITE_TRUNCATE) load GCS files into the `raw` dataset with schema autodetection.
4. **Staging** — Bruin SQL assets clean, type-cast, and rename columns into the `staging` dataset.
5. **Marts** — Bruin SQL mart assets aggregate staging data into 10 analytics-ready tables in the `marts` dataset.
6. **Streaming** — Cloud Scheduler triggers `producer-job` and `consumer-job` every minute. Producer polls the MTR Schedule API and publishes events to Redpanda. Consumer reads from Redpanda and writes to BigQuery `streaming` dataset.
7. **Visualization** — The Streamlit dashboard on Cloud Run queries marts and streaming tables directly via the BigQuery Python client.

---

## Project Structure

```
hk-transit-pulse/
├── Dockerfile                          # Dashboard container image
├── .dockerignore
├── requirements.txt                    # Python dependencies
├── .github/
│   └── workflows/
│       ├── deploy.yml                  # CI/CD: build + deploy dashboard on push to main
│       └── batch.yml                   # CI/CD: build + run batch job (daily at 06:00 HKT)
├── bruin/                              # Batch pipeline (Bruin)
│   ├── Dockerfile                      # Batch container image
│   ├── .bruin.yml                      # Bruin GCP connection config (gitignored)
│   ├── .bruin.yml.example
│   ├── pipeline.yml                    # Pipeline definition + daily schedule
│   └── assets/
│       ├── ingestion/
│       │   ├── ingest_gtfs_static.py   # Download GTFS ZIP -> GCS -> BigQuery
│       │   └── ingest_mtr_csv.py       # Fetch MTR CSVs -> GCS -> BigQuery
│       ├── staging/
│       │   ├── stg_stops.sql
│       │   ├── stg_routes.sql
│       │   ├── stg_trips.sql
│       │   ├── stg_stop_times.sql
│       │   └── stg_calendar.sql
│       └── marts/
│           ├── mart_stops_ranked.sql
│           ├── mart_trips_per_route.sql
│           ├── mart_peak_hour_analysis.sql
│           ├── mart_route_service_hours.sql
│           ├── mart_service_frequency.sql
│           ├── mart_transfer_hubs.sql
│           ├── mart_weekday_vs_weekend.sql
│           ├── mart_longest_routes.sql
│           ├── mart_early_night_routes.sql
│           └── mart_trip_trajectories.sql
├── dashboard/
│   └── app.py                          # Streamlit dashboard (4 tabs)
├── streaming/
│   ├── config.py
│   ├── producer.py                     # One-shot: poll MTR API -> Redpanda
│   └── consumer.py                     # One-shot: Redpanda -> BigQuery
└── terraform/
    ├── main.tf                         # GCS + BigQuery + service account + IAM
    ├── variables.tf
    ├── outputs.tf
    └── terraform.tfvars.example
```

---

## Data Sources

### HK Transport GTFS Static
**URL:** `https://static.data.gov.hk/td/pt-headway-en/gtfs.zip`

| File | Contents |
|---|---|
| routes.txt | Bus, tram, and ferry routes |
| stops.txt | Stop locations + coordinates |
| trips.txt | Individual trips per route |
| stop_times.txt | Arrivals/departures per stop |
| calendar.txt | Weekday vs weekend service days |

> Covers KMB buses, CTB/NWFB citybus, trams, and ferries. Updated daily. MTR does **not** publish GTFS — trip-level data is not publicly available.

### MTR Open Data
**URL:** `https://opendata.mtr.com.hk`

| Table | Contents |
|---|---|
| mtr_lines_stations | Heavy rail lines and stations |
| mtr_bus_stops | MTR feeder bus stops and routes |
| mtr_fares | Station-to-station fare table |
| mtr_light_rail_stops | Light Rail routes and stops |

---

## Data Pipeline

### 1. Ingestion

Two Bruin Python assets handle data extraction:

- `ingest_gtfs_static.py` — Downloads the GTFS ZIP from data.gov.hk, extracts individual files, uploads to GCS, then runs BigQuery load jobs.
- `ingest_mtr_csv.py` — Fetches 4 MTR CSVs, normalises column names, strips UTF-8 BOM, uploads to GCS, then loads to BigQuery.

Both run under the `hk-transit-pipeline` Bruin pipeline on a daily cron schedule.

### 2. Staging

Five Bruin SQL assets clean and standardise raw data:

| Asset | Source | Key transformations |
|---|---|---|
| `stg_stops` | `raw.gtfs_stops` | Rename lat/lon, cast types |
| `stg_routes` | `raw.gtfs_routes` | Map route_type codes |
| `stg_trips` | `raw.gtfs_trips` | Join route metadata |
| `stg_stop_times` | `raw.gtfs_stop_times` | Parse HH:MM:SS times |
| `stg_calendar` | `raw.gtfs_calendar` | Boolean weekday flags |

### 3. Marts

Ten aggregated mart models power the dashboard:

| Mart | Description |
|---|---|
| `mart_stops_ranked` | Stops ranked by total departures |
| `mart_trips_per_route` | Trip count per route |
| `mart_peak_hour_analysis` | Trips by hour of day |
| `mart_route_service_hours` | First and last service per route |
| `mart_service_frequency` | Average headway per route |
| `mart_transfer_hubs` | Stops served by multiple routes |
| `mart_weekday_vs_weekend` | Service count split by day type |
| `mart_longest_routes` | Routes ranked by stop count |
| `mart_early_night_routes` | Routes with earliest/latest services |
| `mart_trip_trajectories` | Route paths for map rendering |

### 4. BigQuery Layout

```
<GCP_PROJECT_ID>/
├── raw/          ← loaded from GCS via BQ load jobs
├── staging/      ← Bruin SQL staging assets
└── marts/        ← Bruin SQL mart assets (aggregated)
```

All datasets are in the **US** region.

---

## Insights & Visualizations

The Streamlit dashboard (`dashboard/app.py`) has 4 tabs:

**Network Analytics** — GTFS-based charts filtered by transport type (Bus, Tram, Ferry, Peak Tram):
- Stop locations map (pydeck)
- Top 10 busiest stops
- Top 20 routes by trip count
- Departures by hour of day
- Weekday vs weekend service comparison
- Transfer hubs, longest routes, early/late services

**MTR Live 港鐵** — MTR-specific data from open data CSVs:
- Heavy rail network (lines and stations)
- Light Rail routes and stops

**Streaming Analytics 實時分析** — Real-time event stream via Kafka:
- Events per MTR line
- Event volume over time
- UP vs DOWN train breakdown

**About** — Project background and data sources.

---

## Steps to Reproduce

### Prerequisites

- [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) (Windows users)
- [Bruin CLI](https://getbruin.com)
- [OpenTofu](https://opentofu.org)
- [gcloud CLI](https://cloud.google.com/sdk/docs/install)
- GCP project with billing enabled
- Python 3.11+

### 1. Clone the Repository

```bash
git clone https://github.com/MFarizalA/hk-transit-pulse.git
cd hk-transit-pulse
```

### 2. GCP Authentication

```bash
gcloud auth application-default login --no-browser
gcloud auth application-default set-quota-project <GCP_PROJECT_ID>
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
export GOOGLE_CLOUD_PROJECT=<GCP_PROJECT_ID>
```

### 3. Provision Infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Fill in your project_id in terraform.tfvars
tofu init && tofu apply
```

This creates the GCS bucket, BigQuery datasets (`raw`, `staging`, `marts`), and a service account.

### 4. Configure Bruin

Copy and edit `.bruin.yml`:

```bash
cp .bruin.yml.example .bruin.yml
```

```yaml
default_environment: default
environments:
  default:
    connections:
      google_cloud_platform:
        - name: gcp
          project_id: <GCP_PROJECT_ID>
          location: US
          use_application_default_credentials: true
```

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 6. Run the Pipeline

```bash
# Validate all assets
bruin validate

# Run full pipeline (ingestion -> staging -> marts)
bruin run

# Or run individual layers
bruin run assets/ingestion/ingest_gtfs_static.py
bruin run assets/ingestion/ingest_mtr_csv.py
bruin run assets/staging/stg_stops.sql
bruin run assets/marts/mart_stops_ranked.sql
```

### 7. Run the Dashboard

```bash
streamlit run dashboard/app.py
```

---

## About

Built by **Rizal**, an Indonesian data engineer based in Jakarta with a keen interest in urban mobility, open government data, and Hong Kong cinema.

📧 Reach out via [GitHub](https://github.com/MFarizalA)
