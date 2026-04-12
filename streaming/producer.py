"""
HK Transit Pulse — Streaming Producer
Polls KMB bus ETA + MTR schedule APIs every 30s and publishes events to Redpanda.
"""
import json
import time
import logging
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config import (
    REDPANDA_BROKERS, REDPANDA_USERNAME, REDPANDA_PASSWORD,
    TOPIC_BUS_ETA, TOPIC_MTR, POLL_INTERVAL,
    KMB_STOP_IDS, MTR_LINE_STATIONS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KMB_STOP_ETA_URL = "https://data.etabus.gov.hk/v1/transport/kmb/stop-eta/{stop_id}"
MTR_SCHEDULE_URL = "https://rt.data.gov.hk/v1/transport/mtr/getSchedule.php"


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": REDPANDA_BROKERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": REDPANDA_USERNAME,
        "sasl.password": REDPANDA_PASSWORD,
        "client.id": "hk-transit-producer",
    })


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed: %s", err)


def fetch_kmb_eta(stop_id: str) -> list[dict]:
    """Fetch next bus ETAs for a KMB stop."""
    try:
        resp = requests.get(KMB_STOP_ETA_URL.format(stop_id=stop_id), timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        return [
            {
                "event_type": "bus_eta",
                "stop_id": stop_id,
                "route": item.get("route"),
                "direction": item.get("dir"),
                "service_type": item.get("service_type"),
                "destination_en": item.get("dest_en"),
                "destination_tc": item.get("dest_tc"),
                "eta_seq": item.get("eta_seq"),
                "eta": item.get("eta"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            for item in data
            if item.get("eta")
        ]
    except Exception as e:
        log.warning("KMB ETA fetch failed for stop %s: %s", stop_id, e)
        return []


def fetch_mtr_schedule(line: str, station: str) -> list[dict]:
    """Fetch next train schedule for an MTR line/station."""
    try:
        resp = requests.get(MTR_SCHEDULE_URL, params={"line": line, "sta": station}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != 1:
            return []

        events = []
        station_data = data.get("data", {}).get(f"{line}-{station}", {})
        for direction in ("UP", "DOWN"):
            for train in station_data.get(direction, []):
                if train.get("valid") == "Y":
                    events.append({
                        "event_type": "mtr_schedule",
                        "line": line,
                        "station": station,
                        "direction": direction,
                        "destination": train.get("dest"),
                        "platform": train.get("plat"),
                        "arrival_time": train.get("time"),
                        "minutes_away": train.get("ttnt"),
                        "is_delayed": data.get("isdelay") == "Y",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
        return events
    except Exception as e:
        log.warning("MTR schedule fetch failed for %s-%s: %s", line, station, e)
        return []


def publish_events(producer: Producer, topic: str, events: list[dict]):
    for event in events:
        producer.produce(
            topic=topic,
            key=event.get("stop_id") or f"{event.get('line')}-{event.get('station')}",
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
    producer.poll(0)


def main():
    log.info("Starting HK Transit Pulse producer...")
    producer = make_producer()

    while True:
        poll_start = time.time()

        # ── KMB bus ETAs ──────────────────────────────────────────────────────
        total_bus_events = 0
        for stop_id in KMB_STOP_IDS:
            events = fetch_kmb_eta(stop_id)
            publish_events(producer, TOPIC_BUS_ETA, events)
            total_bus_events += len(events)
        log.info("Published %d bus ETA events", total_bus_events)

        # ── MTR schedules ─────────────────────────────────────────────────────
        total_mtr_events = 0
        for line, station in MTR_LINE_STATIONS:
            events = fetch_mtr_schedule(line, station)
            publish_events(producer, TOPIC_MTR, events)
            total_mtr_events += len(events)
        log.info("Published %d MTR schedule events", total_mtr_events)

        producer.flush()

        elapsed = time.time() - poll_start
        sleep_time = max(0, POLL_INTERVAL - elapsed)
        log.info("Poll done in %.1fs. Sleeping %.1fs...", elapsed, sleep_time)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
