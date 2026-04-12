# Tech Debt

Known shortcuts, rough edges, and things to fix after the competition deadline.

---

## Current Sprint (Before April 21 Deadline)

| # | Task | Est. Effort | Status |
|---|---|---|---|
| S1 | Streamlit dashboard | ~2-3 hrs | [x] Done |
| S2 | Data quality checks on staging assets | ~1 hr | [ ] |
| S3 | MTR station data — ingestion + staging + mart | ~2-3 hrs | [x] Done |

---

## Future Improvements (Post-Deadline / Attempt 2)

### Exciting Ideas
These are interesting but unrealistic for the current deadline:

- **Real-time GTFS** — live vehicle positions, trip updates via GTFS-RT + Pub/Sub + Dataflow. Completely different stack from batch.
- **Ferry routes** — no clean GTFS feed available; would need scraping or manual CSV cleaning.
- **Cross-modal hub analysis** — geospatial joins between bus stops + MTR stations to find transfer points.

---

## High Priority Tech Debt

### 1. GCP Project ID hardcoded in SQL assets
All SQL assets reference the project ID directly:
```sql
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_stops`
```
Should be parameterized via an environment variable or Bruin variable so the pipeline is portable.

### 2. GCP Project ID hardcoded in ingestion scripts
`GCP_PROJECT = "project-e5d4de8a-49cc-439d-b6e"` in both ingestion scripts.
Should be read from environment variable: `os.environ["GOOGLE_CLOUD_PROJECT"]`

### 3. No data quality checks
Staging assets have no `columns` checks (not_null, unique) in Bruin headers.
Data silently passes even if primary keys are null or duplicated.
**Fix:** Add Bruin column checks to all staging assets.

### ~~4. requirements.txt has no pinned versions~~ ✓ RESOLVED
All core packages pinned.

---

## Medium Priority

### 5. Ingestion downloads full ZIP every run
The pipeline always re-downloads the full GTFS ZIP and re-loads all tables even if data hasn't changed.
**Fix:** Check GCS object metadata (e.g. ETag or Last-Modified) before re-downloading.

### 6. BigQuery load uses WRITE_TRUNCATE on every run
All raw tables are fully replaced on every ingestion run.
**Fix:** For production, consider incremental loads or at minimum a timestamp partition.

### 7. `shapes.txt` silently skipped
The HK GTFS ZIP does not include `shapes.txt`. The ingestion script skips it with a WARNING print.
**Fix:** Remove `shapes.txt` from the expected files list.

### 8. No pipeline failure alerting
If `bruin run` fails, there is no notification.
**Fix:** Add email or Slack alerting on failure via Bruin hooks or a wrapper script.

### 9. KMB stop IDs in streaming/config.py are placeholders
Producer is generating 0 bus ETA events because stop IDs are not real KMB stop IDs.
**Fix:** Replace placeholder stop IDs with actual KMB stop IDs from the GTFS stops table.

---

## Low Priority

### 10. ~~`.bruin.yml` is gitignored but has no example file~~ ✓ RESOLVED
Added `.bruin.yml.example` to the repo.

### 11. BigQuery datasets have `delete_contents_on_destroy = true`
Convenient for dev but dangerous in production — a `tofu destroy` would wipe all data.
**Fix:** Set to `false` after the competition.

### 12. No partitioning on `stg_stop_times`
`stop_times` is the largest table. Unpartitioned queries scan the full table every run.
**Fix:** Partition by `departure_time` (hour) or cluster by `trip_id`.

### 13. Mart SQL uses full project path instead of dataset alias
All mart queries use full `project.dataset.table` paths. If the project changes, all files need updating.
**Fix:** Consolidate project ID into a single config or use Bruin variables once supported.
