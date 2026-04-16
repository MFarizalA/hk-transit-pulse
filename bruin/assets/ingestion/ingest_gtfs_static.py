"""@bruin
name: raw.gtfs_static_hk-transport
type: python
@bruin"""

import io
import os
import zipfile

import requests
from google.cloud import bigquery, storage

GTFS_URL = "https://static.data.gov.hk/td/pt-headway-en/gtfs.zip"
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "hk-transit-pulse-raw")
GCS_PREFIX  = "gtfs_static/hk-transport"
GCP_PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BQ_DATASET  = "raw"

# Maps filename -> BigQuery table name
GTFS_FILES = {
    "routes.txt": "gtfs_routes",
    "stops.txt": "gtfs_stops",
    "trips.txt": "gtfs_trips",
    "stop_times.txt": "gtfs_stop_times",
    "calendar.txt": "gtfs_calendar",
}


def main():
    print(f"Downloading GTFS from {GTFS_URL}")
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()

    gcs_client = storage.Client(project=GCP_PROJECT)
    bq_client = bigquery.Client(project=GCP_PROJECT)
    bucket = gcs_client.bucket(GCS_BUCKET)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for filename, bq_table in GTFS_FILES.items():
            if filename not in zf.namelist():
                print(f"WARNING: {filename} not found in ZIP, skipping")
                continue

            # Upload to GCS
            data = zf.read(filename)
            gcs_path = f"{GCS_PREFIX}/{filename}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(data, content_type="text/plain")
            print(f"Uploaded {filename} -> gs://{GCS_BUCKET}/{gcs_path}")

            # Load from GCS into BigQuery
            gcs_uri = f"gs://{GCS_BUCKET}/{gcs_path}"
            table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{bq_table}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job.result()
            print(f"Loaded {filename} -> {table_ref}")

    print("Ingestion complete.")


main()
