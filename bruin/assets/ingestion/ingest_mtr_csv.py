"""@bruin
name: raw.mtr_static
type: python
@bruin"""

import io
import os

import requests
import pandas as pd
from google.cloud import bigquery, storage

GCS_BUCKET  = os.environ.get("GCS_BUCKET", "hk-transit-pulse-raw")
GCS_PREFIX  = "mtr_static"
GCP_PROJECT = os.environ["GOOGLE_CLOUD_PROJECT"]
BQ_DATASET  = "raw"

MTR_SOURCES = {
    "mtr_lines_stations":    "https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv",
    "mtr_bus_stops":         "https://opendata.mtr.com.hk/data/mtr_bus_stops.csv",
    "mtr_fares":             "https://opendata.mtr.com.hk/data/mtr_lines_fares.csv",
    "mtr_light_rail_stops":  "https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv",
}

# Raw CSV column → BQ snake_case column
COLUMN_MAPS = {
    "mtr_lines_stations": {
        "Line Code":    "line_code",
        "Station Code": "station_code",
        "English Name": "station_name_en",
        "Chinese Name": "station_name_zh",
    },
    "mtr_bus_stops": {
        "ROUTE_ID":          "route_id",
        "ROUTE_NAMEE":       "route_name_en",
        "ROUTE_NAMEC":       "route_name_zh",
        "DIRECTION":         "direction",
        "STATION_SEQUENCE":  "station_sequence",
        "STATION_ID":        "station_id",
        "STATION_NAME_ENG":  "station_name_en",
        "STATION_NAME_CHI":  "station_name_zh",
        "STATION_LATITUDE":  "latitude",
        "STATION_LONGITUDE": "longitude",
    },
    "mtr_fares": {
        "LINE":             "line_code",
        "SRC_STATION_ID":   "origin_station_id",
        "DST_STATION_ID":   "destination_station_id",
        "CLASS":            "fare_class",
        "OCT_ADT_FARE":     "oct_adult_fare",
        "OCT_STD_FARE":     "oct_student_fare",
        "OCT_CON_FARE":     "oct_concessionary_fare",
        "SINGLE_ADT_FARE":  "single_adult_fare",
        "SINGLE_STD_FARE":  "single_student_fare",
        "SINGLE_CON_FARE":  "single_concessionary_fare",
    },
    "mtr_light_rail_stops": {
        "ROUTE_ID":          "route_id",
        "ROUTE_NAMEE":       "route_name_en",
        "ROUTE_NAMEC":       "route_name_zh",
        "DIRECTION":         "direction",
        "STOP_SEQUENCE":     "stop_sequence",
        "STATION_ID":        "station_id",
        "STATION_NAMEE":     "station_name_en",
        "STATION_NAMEC":     "station_name_zh",
        "STATION_LATITUDE":  "latitude",
        "STATION_LONGITUDE": "longitude",
    },
}


HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; hk-transit-pulse/1.0)"}


def fetch_and_normalise(url: str, col_map: dict) -> bytes:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    # utf-8-sig strips the BOM (\ufeff) that MTR CSVs prepend
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")))
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns=col_map)
    # Drop unnamed trailing columns the CSVs sometimes have
    df = df[[c for c in df.columns if not c.startswith("Unnamed")]]
    return df.to_csv(index=False).encode()


def main():
    gcs_client = storage.Client(project=GCP_PROJECT)
    bq_client  = bigquery.Client(project=GCP_PROJECT)
    bucket     = gcs_client.bucket(GCS_BUCKET)

    for table_name, url in MTR_SOURCES.items():
        print(f"→ {table_name}: fetching {url}")
        csv_bytes = fetch_and_normalise(url, COLUMN_MAPS[table_name])

        # Upload normalised CSV to GCS
        gcs_path = f"{GCS_PREFIX}/{table_name}.csv"
        bucket.blob(gcs_path).upload_from_string(csv_bytes, content_type="text/plain")
        print(f"  Uploaded -> gs://{GCS_BUCKET}/{gcs_path}")

        # Load GCS -> BigQuery (truncate-and-reload)
        gcs_uri   = f"gs://{GCS_BUCKET}/{gcs_path}"
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        tbl = bq_client.get_table(table_ref)
        print(f"  Loaded   -> {table_ref}  ({tbl.num_rows:,} rows)")

    print("MTR static ingestion complete.")


main()
