import os

# ── Redpanda Cloud connection ──────────────────────────────────────────────────
REDPANDA_BROKERS   = os.environ["REDPANDA_BROKERS"]
REDPANDA_USERNAME  = os.environ["REDPANDA_USERNAME"]
REDPANDA_PASSWORD  = os.environ["REDPANDA_PASSWORD"]

# ── Topics ────────────────────────────────────────────────────────────────────
TOPIC_BUS_ETA  = "hk-bus-eta"
TOPIC_MTR      = "hk-mtr-schedule"

# ── GCP ───────────────────────────────────────────────────────────────────────
PROJECT_ID  = "project-e5d4de8a-49cc-439d-b6e"
BQ_DATASET  = "streaming"

# ── Poll interval (seconds) ───────────────────────────────────────────────────
POLL_INTERVAL = 30

# ── KMB stops to poll (top busy stops from our batch data) ────────────────────
# These stop IDs come from the KMB stops API (name matched to our GTFS top stops)
KMB_STOP_IDS = [
    "B06E0B6A8C3E0C40",  # Mong Kok
    "DB0D12A6BD7C7B7E",  # Tsim Sha Tsui
    "9F5F3F3E5B9E0B0E",  # Central
    "D42B3D00F32D3C12",  # Causeway Bay
    "B11A9C7E5B9E0B0E",  # Wan Chai
]

# ── MTR lines + terminal stations to poll ─────────────────────────────────────
MTR_LINE_STATIONS = [
    ("TML", "TUM"),   # Tuen Ma Line - Tuen Mun
    ("TCL", "TUC"),   # Tung Chung Line - Tung Chung
    ("KTL", "KWT"),   # Kwun Tong Line - Kwun Tong
    ("TWL", "TSW"),   # Tsuen Wan Line - Tsuen Wan
    ("ISL", "KET"),   # Island Line - Kennedy Town
    ("EAL", "LOW"),   # East Rail Line - Lo Wu
    ("AEL", "AWE"),   # Airport Express - AsiaWorld-Expo
    ("SIL", "OCP"),   # South Island Line - Ocean Park
    ("TKL", "POA"),   # Tseung Kwan O Line - Po Lam
    ("DRL", "DIS"),   # Disneyland Resort Line
]
