# Tech Debt

Known shortcuts, rough edges, and things to fix after the competition deadline.

---

## High Priority

### 1. GCP Project ID hardcoded in SQL assets
All SQL assets reference the project ID directly:
```sql
FROM `project-e5d4de8a-49cc-439d-b6e.raw.gtfs_stops`
```
Should be parameterized via an environment variable or Bruin variable so the pipeline is portable.

### 2. GCP Project ID hardcoded in ingestion script
`GCP_PROJECT = "project-e5d4de8a-49cc-439d-b6e"` in `assets/ingestion/ingest_gtfs_static.py`.
Should be read from environment variable: `os.environ["GOOGLE_CLOUD_PROJECT"]`

### 3. No data quality checks
Staging assets have no `columns` checks (not_null, unique) in Bruin headers.
Data silently passes even if primary keys are null or duplicated.
**Fix:** Add Bruin column checks to all staging assets.

### ~~4. requirements.txt has no pinned versions~~ ✓ RESOLVED
Pinned to: `requests==2.33.1`, `google-cloud-storage==3.10.1`, `google-cloud-bigquery==3.41.0`

---

## Medium Priority

### 5. Ingestion downloads full ZIP every run
The pipeline always re-downloads the full GTFS ZIP (~10MB+) and re-loads all tables even if data hasn't changed.
**Fix:** Check GCS object metadata (e.g. ETag or Last-Modified) before re-downloading.

### 6. BigQuery load uses WRITE_TRUNCATE on every run
All raw tables are fully replaced on every ingestion run.
**Fix:** For production, consider incremental loads or at minimum a timestamp partition.

### 7. `shapes.txt` silently skipped
The HK GTFS ZIP does not include `shapes.txt`. The ingestion script skips it with a WARNING print.
This means route geometry is unavailable.
**Fix:** Document this limitation clearly; remove `shapes.txt` from the expected files list or handle gracefully.

### 8. No pipeline failure alerting
If `bruin run` fails, there is no notification.
**Fix:** Add email or Slack alerting on failure (can be done via Bruin hooks or a wrapper script).

---

## Low Priority

### 9. `.bruin.yml` is gitignored but has no example file
New contributors have no reference for the correct `.bruin.yml` format.
**Fix:** Add a `.bruin.yml.example` similar to `terraform.tfvars.example`.

### 10. BigQuery datasets have `delete_contents_on_destroy = true`
Convenient for dev but dangerous in production — a `tofu destroy` would wipe all data.
**Fix:** Set to `false` after the competition; rely on manual table deletion when needed.

### 11. No partitioning on `stg_stop_times`
`stop_times` is the largest table. Unpartitioned queries scan the full table every run.
**Fix:** Partition by `departure_time` (hour) or cluster by `trip_id` to reduce query cost.

### 12. Mart SQL uses full project path instead of dataset alias
All mart queries use full `project.dataset.table` paths. If the project changes, all files need updating.
**Fix:** Consolidate project ID into a single config or use Bruin variables once supported.
