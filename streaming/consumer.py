"""
HK Transit Pulse — Streaming Consumer
Reads events from Redpanda and writes to BigQuery streaming tables.
"""
import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery

from config import (
    REDPANDA_BROKERS, REDPANDA_USERNAME, REDPANDA_PASSWORD,
    TOPIC_BUS_ETA, TOPIC_MTR, PROJECT_ID, BQ_DATASET,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BQ_TABLE_BUS  = f"{PROJECT_ID}.{BQ_DATASET}.bus_eta_raw"
BQ_TABLE_MTR  = f"{PROJECT_ID}.{BQ_DATASET}.mtr_schedule_raw"
BATCH_SIZE    = 50   # rows to buffer before inserting into BQ


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": REDPANDA_BROKERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": REDPANDA_USERNAME,
        "sasl.password": REDPANDA_PASSWORD,
        "group.id": "hk-transit-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def insert_rows(bq_client: bigquery.Client, table: str, rows: list[dict]):
    if not rows:
        return
    errors = bq_client.insert_rows_json(table, rows)
    if errors:
        log.error("BQ insert errors for %s: %s", table, errors)
    else:
        log.info("Inserted %d rows into %s", len(rows), table)


RUN_DURATION = 50  # seconds to drain the queue before exiting


def main():
    log.info("Starting HK Transit Pulse consumer (one-shot, %ds window)...", RUN_DURATION)
    consumer = make_consumer()
    bq_client = bigquery.Client(project=PROJECT_ID)

    consumer.subscribe([TOPIC_BUS_ETA, TOPIC_MTR])

    bus_buffer: list[dict] = []
    mtr_buffer: list[dict] = []
    deadline = time.time() + RUN_DURATION

    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                log.warning("Bad JSON: %s", e)
                continue

            topic = msg.topic()
            if topic == TOPIC_BUS_ETA:
                bus_buffer.append(event)
                if len(bus_buffer) >= BATCH_SIZE:
                    insert_rows(bq_client, BQ_TABLE_BUS, bus_buffer)
                    bus_buffer.clear()

            elif topic == TOPIC_MTR:
                mtr_buffer.append(event)
                if len(mtr_buffer) >= BATCH_SIZE:
                    insert_rows(bq_client, BQ_TABLE_MTR, mtr_buffer)
                    mtr_buffer.clear()

    finally:
        # Flush remaining
        if bus_buffer:
            insert_rows(bq_client, BQ_TABLE_BUS, bus_buffer)
        if mtr_buffer:
            insert_rows(bq_client, BQ_TABLE_MTR, mtr_buffer)
        consumer.close()
        log.info("Consumer done. Flushed %d bus + %d MTR rows to BQ.", len(bus_buffer), len(mtr_buffer))


if __name__ == "__main__":
    main()
