"""
Kafka producer for streamflow.

Replays pre-generated synthetic inventory/checkout events from a file (default:
data/pregenerated_events.jsonl) and publishes them to a Kafka topic.

Configuration (overridable via environment variables/config file):
    KAFKA_BOOTSTRAP_SERVERS    default "localhost:9092"
    KAFKA_TOPIC                default "streamflow.events"
    EVENTS_PER_SECOND          default 2   (pacing rate)
    LOG_LEVEL                  default "INFO"
    PREGENERATED_EVENTS_PATH   default "data/pregenerated_events.jsonl"
"""

from __future__ import annotations

import json
import logging
import os
import signal
import time
from pathlib import Path

import yaml
from kafka import KafkaProducer

logger = logging.getLogger("streamflow.producer")


def _load_pipeline_config() -> dict:
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    config_paths = [
        "/opt/streamflow/config/pipeline.yml",
        project_root / "config" / "pipeline.yml",
        "config/pipeline.yml"
    ]
    config = {}
    for path in config_paths:
        try:
            path = Path(path)
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                break
        except Exception:
            pass
    return config


def _build_kafka_producer(config: dict) -> KafkaProducer:
    bootstrap_servers = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        config.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    )
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        value_serializer=lambda value: value.encode("utf-8") if isinstance(value, str) else json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key is not None else None,
        linger_ms=50,
        retries=5,
    )


def start_producer() -> None:
    config = _load_pipeline_config()

    log_level = os.environ.get("LOG_LEVEL", config.get("producer", {}).get("log_level", "INFO"))
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    topic = os.environ.get("KAFKA_TOPIC", config.get("kafka", {}).get("topic", "streamflow.events"))
    events_per_second = float(os.environ.get("EVENTS_PER_SECOND", config.get("producer", {}).get("events_per_second", "2")))
    sleep_interval = 1.0 / events_per_second if events_per_second > 0 else 0.0

    # Locate the pre-generated events file
    pregen_path_str = os.environ.get(
        "PREGENERATED_EVENTS_PATH",
        config.get("producer", {}).get("pregenerated_events_path", "data/pregenerated_events.jsonl")
    )

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    
    pregen_path = Path(pregen_path_str)
    if not pregen_path.exists():
        # Try resolving relative to project root
        pregen_path = project_root / pregen_path_str
        if not pregen_path.exists():
            # Try /opt/streamflow prefix (Docker standard)
            pregen_path = Path("/opt/streamflow") / pregen_path_str

    if not pregen_path.exists():
        logger.error("Pregenerated events file not found at: %s", pregen_path_str)
        raise FileNotFoundError(f"Pregenerated events file not found at: {pregen_path_str}")

    logger.info("Loading pre-generated events from: %s", pregen_path)
    with open(pregen_path, "r", encoding="utf-8") as f:
        event_lines = [line.strip() for line in f if line.strip()]

    logger.info(
        "Starting producer: topic=%s events_per_second=%s total_events=%d",
        topic, events_per_second, len(event_lines),
    )

    producer = _build_kafka_producer(config)

    stop_requested = False

    def _handle_signal(signum, _frame):
        nonlocal stop_requested
        logger.info("Received signal %s, finishing current send and shutting down...", signum)
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    sent = 0
    try:
        for line in event_lines:
            if stop_requested:
                break

            # Try to extract event key from JSON structure to enable partition routing
            key = None
            try:
                event_obj = json.loads(line)
                key = event_obj.get("entity_id")
                if not key and "payload" in event_obj and event_obj["payload"]:
                    key = event_obj["payload"].get("sku_id")
            except Exception:
                # Use None key if JSON is malformed
                pass

            producer.send(topic, key=key, value=line)
            sent += 1

            if sent % 100 == 0 or sent == len(event_lines):
                logger.info("Sent %d / %d events to topic '%s'", sent, len(event_lines), topic)

            if sleep_interval:
                time.sleep(sleep_interval)
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped after sending %d event(s).", sent)


if __name__ == "__main__":
    start_producer()
