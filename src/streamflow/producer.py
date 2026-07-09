"""
Kafka producer for streamflow.

Continuously generates synthetic inventory/checkout events and publishes them
as JSON to a Kafka topic. Matches the event contract in
streamflow.schemas.RETAIL_SPARK_SCHEMA and the allow-lists enforced by
streamflow.quality.validate_dataframe (event_type, source).

Configuration (all overridable via environment variables so nothing here is
hardcoded per project_req.md):

    KAFKA_BOOTSTRAP_SERVERS  default "localhost:9092"
    KAFKA_TOPIC              default "streamflow.events"
    EVENTS_PER_SECOND        default 2   (pacing of the continuous loop)
    LOG_LEVEL                default "INFO"

Inventory model
---------------
A fixed catalog of SKUs (SKU_CATALOG below) is the single source of truth for
product identity and price -- every event for a given sku_id always reports
the same price, so "the same product" never has two different prices across
events. Each SKU also has a running `current_stock_level` held in memory,
which this process is the sole writer of:

  * A "sale" (quantity_delta < 0) is only generated for SKUs with stock > 0,
    and decrements that SKU's level. Sales carry a checkout_lane.
  * A "restock" (quantity_delta > 0) increments the level and never carries
    a checkout_lane (restocks don't happen at a checkout).

So current_stock_level in each event is always a true running total for that
SKU, not an independently-randomized number.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from kafka import KafkaProducer

logger = logging.getLogger("streamflow.producer")

EVENT_TYPE = "inventory.stock_change"
SOURCE = "streamflow-producer"

CHECKOUT_LANES = [
    "lane-1",
    "lane-2",
    "lane-3",
    "lane-4",
    "self-checkout-1",
    "self-checkout-2",
]

# Fixed product catalog: sku_id -> (price, starting_stock). This is the only
# place prices are defined, so every event for a SKU is priced consistently.
SKU_CATALOG = {
    "SKU-00001": {"price": 12.99, "starting_stock": 150},
    "SKU-00002": {"price": 4.50, "starting_stock": 90},
    "SKU-00003": {"price": 19.99, "starting_stock": 200},
    "SKU-00004": {"price": 7.25, "starting_stock": 60},
    "SKU-00005": {"price": 34.00, "starting_stock": 135},
    "SKU-00006": {"price": 99.99, "starting_stock": 20},
    "SKU-00007": {"price": 2.99, "starting_stock": 300},
    "SKU-00008": {"price": 15.50, "starting_stock": 45},
    "SKU-00009": {"price": 8.75, "starting_stock": 80},
    "SKU-00010": {"price": 27.30, "starting_stock": 125},
}

# Running stock ledger, seeded from the catalog. This process is the only
# writer, so it stays a true, coherent inventory count across all events.
_stock_levels = {sku: info["starting_stock"] for sku, info in SKU_CATALOG.items()}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def generate_inventory_event(rng: random.Random | None = None) -> dict:
    """Generate one event and apply its effect to the in-memory stock ledger."""
    rng = rng or random

    # Only allow a sale for SKUs that currently have stock; otherwise force a
    # restock so levels can never go negative and out-of-stock SKUs recover.
    sku_id = rng.choice(list(SKU_CATALOG.keys()))
    can_sell = _stock_levels[sku_id] > 0
    is_sale = can_sell and rng.random() < 0.8  # sales are more common than restocks

    if is_sale:
        max_sale = min(_stock_levels[sku_id], 5)
        quantity_delta = -rng.randint(1, max_sale)
        checkout_lane = rng.choice(CHECKOUT_LANES)
    else:
        quantity_delta = rng.randint(1, 20)
        checkout_lane = None

    _stock_levels[sku_id] += quantity_delta

    payload = {
        "sku_id": sku_id,
        "quantity_delta": quantity_delta,
        "current_stock_level": _stock_levels[sku_id],
        "checkout_lane": checkout_lane,
        "price": SKU_CATALOG[sku_id]["price"],
    }

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": EVENT_TYPE,
        "event_ts": _now_iso(),
        "source": SOURCE,
        "entity_id": sku_id,
        "payload": payload,
    }


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
                with open(path, "r") as f:
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
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
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

    logger.info(
        "Starting producer: topic=%s events_per_second=%s skus=%d",
        topic, events_per_second, len(SKU_CATALOG),
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
        while not stop_requested:
            event = generate_inventory_event()
            producer.send(topic, key=event["entity_id"], value=event)
            sent += 1

            if sent % 20 == 0:
                logger.info("Sent %d events to topic '%s'", sent, topic)

            if sleep_interval:
                time.sleep(sleep_interval)
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer stopped after sending %d event(s).", sent)


if __name__ == "__main__":
    start_producer()
