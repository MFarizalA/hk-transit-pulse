"""
Run once to create the BigQuery streaming dataset and tables.
Usage: python create_bq_tables.py
"""
from google.cloud import bigquery
from config import PROJECT_ID, BQ_DATASET

client = bigquery.Client(project=PROJECT_ID)

# Create dataset
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
dataset_ref.location = "asia-east1"
client.create_dataset(dataset_ref, exists_ok=True)
print(f"Dataset {BQ_DATASET} ready.")

# ── bus_eta_raw ────────────────────────────────────────────────────────────────
bus_schema = [
    bigquery.SchemaField("event_type",      "STRING"),
    bigquery.SchemaField("stop_id",         "STRING"),
    bigquery.SchemaField("route",           "STRING"),
    bigquery.SchemaField("direction",       "STRING"),
    bigquery.SchemaField("service_type",    "STRING"),
    bigquery.SchemaField("destination_en",  "STRING"),
    bigquery.SchemaField("destination_tc",  "STRING"),
    bigquery.SchemaField("eta_seq",         "INTEGER"),
    bigquery.SchemaField("eta",             "STRING"),
    bigquery.SchemaField("timestamp",       "TIMESTAMP"),
]
bus_table = bigquery.Table(f"{PROJECT_ID}.{BQ_DATASET}.bus_eta_raw", schema=bus_schema)
client.create_table(bus_table, exists_ok=True)
print("Table bus_eta_raw ready.")

# ── mtr_schedule_raw ──────────────────────────────────────────────────────────
mtr_schema = [
    bigquery.SchemaField("event_type",    "STRING"),
    bigquery.SchemaField("line",          "STRING"),
    bigquery.SchemaField("station",       "STRING"),
    bigquery.SchemaField("direction",     "STRING"),
    bigquery.SchemaField("destination",   "STRING"),
    bigquery.SchemaField("platform",      "STRING"),
    bigquery.SchemaField("arrival_time",  "STRING"),
    bigquery.SchemaField("minutes_away",  "STRING"),
    bigquery.SchemaField("is_delayed",    "BOOLEAN"),
    bigquery.SchemaField("timestamp",     "TIMESTAMP"),
]
mtr_table = bigquery.Table(f"{PROJECT_ID}.{BQ_DATASET}.mtr_schedule_raw", schema=mtr_schema)
client.create_table(mtr_table, exists_ok=True)
print("Table mtr_schedule_raw ready.")

print("All streaming tables created.")
