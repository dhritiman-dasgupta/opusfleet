"""Configuration for the pipeline. Everything is env-driven so the same image
runs locally under compose, on the VPS, and against real AWS S3.
"""

import os


def _int(name, default):
    return int(os.environ.get(name, default))


def _bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# ---- ingest ----
INGEST_HOST = os.environ.get("INGEST_HOST", "0.0.0.0")
INGEST_PORT = _int("INGEST_PORT", 6000)
INGEST_BACKLOG = _int("INGEST_BACKLOG", 2048)          # 500+ devices reconnecting at once

# ---- kafka ----
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_AUDIO = os.environ.get("TOPIC_AUDIO", "audio.raw")
TOPIC_STAT = os.environ.get("TOPIC_STAT", "device.stat")
TOPIC_EVENTS = os.environ.get("TOPIC_EVENTS", "device.events")
TOPIC_LEVELS = os.environ.get("TOPIC_LEVELS", "device.levels")
# Opus is already compressed; a second pass costs CPU and saves ~nothing.
KAFKA_COMPRESSION = os.environ.get("KAFKA_COMPRESSION", "none")
KAFKA_LINGER_MS = _int("KAFKA_LINGER_MS", 20)          # one 20 ms frame of batching
KAFKA_ACKS = os.environ.get("KAFKA_ACKS", "1")

# ---- storage (MinIO, or any S3-compatible endpoint) ----
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "opusfleet")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "opusfleet-dev-secret")
S3_BUCKET = os.environ.get("S3_BUCKET", "opusfleet-recordings")
S3_SECURE = _bool("S3_SECURE", False)
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

# ---- segmenter ----
SEGMENT_SECONDS = _int("SEGMENT_SECONDS", 60)
# Store Opus as-is inside an Ogg container: 12x smaller than the WAV the original
# server wrote, and still directly playable. Set to "wav" to reproduce the
# legacy S3 layout (decodes on the server, ~345 MB/hour/device).
SEGMENT_FORMAT = os.environ.get("SEGMENT_FORMAT", "opus")

# ---- live / api ----
WS_HOST = os.environ.get("WS_HOST", "0.0.0.0")
WS_PORT = _int("WS_PORT", 8081)
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = _int("API_PORT", 8080)
PRESIGN_EXPIRY = _int("PRESIGN_EXPIRY", 3600)

# ---- audio (mirrors the firmware; see protocol.py) ----
TIMELINE_DEPTH = _int("TIMELINE_DEPTH", 900)           # ~15 min of per-second levels
