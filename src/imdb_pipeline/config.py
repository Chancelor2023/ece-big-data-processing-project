"""Shared locations and settings for independently developed project jobs."""

import os
from pathlib import Path

PROJECT_ROOT = Path(
    os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2])
)
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"
ENRICHMENT_DIR = PROJECT_ROOT / "data" / "enrichment"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints"
RESULTS_DIR = PROJECT_ROOT / "results"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"
)
WIKIMEDIA_TOPIC = os.environ.get("WIKIMEDIA_TOPIC", "wikimedia.recentchange")
