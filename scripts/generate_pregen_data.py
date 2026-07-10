#!/usr/bin/env python3
"""
generate_pregen_data.py

Simulates a realistic 12-hour retail business day by generating synthetic events.
Saves the pre-generated event stream to:
  data/pregenerated_events.jsonl

The script preserves the existing catalog data in `data/metadata/catalog.csv` and:
  1. Reads the list of product SKUs from `data/metadata/catalog.csv`.
  2. Loads pricing and starting stock configuration from `config/producer.yml`.
  3. Simulates a 12-hour business day (08:00 to 20:00 UTC) with sales peak distributions (lunch & evening rushes).
  4. Generates 90% valid data with real-time stock level tracking, auto-triggering restocks (+50 units) when a SKU's stock drops below 15.
  5. Injects 10% bad data containing synthetic errors to test Spark structured streaming validation rules.
"""

import argparse
import csv
import json
import random
import uuid
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import yaml

# Establish project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CATALOG_PATH = PROJECT_ROOT / "data" / "metadata" / "catalog.csv"
DEFAULT_PRODUCER_CONFIG_PATH = PROJECT_ROOT / "config" / "producer.yml"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "pregenerated_events.jsonl"

CHECKOUT_LANES = [
    "lane-1",
    "lane-2",
    "lane-3",
    "lane-4",
    "self-checkout-1",
    "self-checkout-2",
]


def load_catalog(catalog_path: Path) -> list:
    """Loads product SKUs from the CSV catalog."""
    if not catalog_path.exists():
        print(f"Error: Catalog file not found at {catalog_path}", file=sys.stderr)
        sys.exit(1)

    skus = []
    with open(catalog_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            skus.append({
                "sku_id": row["sku_id"],
                "product_name": row["product_name"],
                "category": row["category"]
            })
    return skus


def load_producer_config(config_path: Path) -> dict:
    """Loads SKU pricing and starting stock from config/producer.yml."""
    if not config_path.exists():
        print(f"Warning: Producer config not found at {config_path}. Using defaults.", file=sys.stderr)
        return {}

    with open(config_path, mode="r", encoding="utf-8") as f:
        try:
            return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Failed to parse YAML from {config_path}: {e}. Using defaults.", file=sys.stderr)
            return {}


def generate_bad_event(ts: str, valid_event_ids: list, sku_ids: list, sku_details: dict) -> str:
    """Generates an invalid event string injecting a specific synthetic error."""
    error_type = random.choice([
        "malformed_json",
        "missing_event_id",
        "missing_event_type",
        "missing_event_ts",
        "missing_source",
        "missing_payload",
        "invalid_event_type",
        "invalid_source",
        "bad_timestamp_format",
        "duplicate_id"
    ])

    # Fallback to other missing field checks if no valid IDs exist yet for duplicates
    if error_type == "duplicate_id" and not valid_event_ids:
        error_type = "missing_event_id"

    sku = random.choice(sku_ids)
    price = sku_details[sku]["price"]

    # Base valid object structure
    base_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "inventory.stock_change",
        "event_ts": ts,
        "source": "streamflow-producer",
        "entity_id": sku,
        "payload": {
            "sku_id": sku,
            "quantity_delta": -1,
            "current_stock_level": 50,
            "checkout_lane": "lane-1",
            "price": price
        }
    }

    if error_type == "malformed_json":
        raw_str = json.dumps(base_event)
        # Corrupt JSON syntax by truncating the end and adding raw malformed content
        return raw_str[:-5] + ',"corrupt":'

    elif error_type == "missing_event_id":
        base_event.pop("event_id")
        return json.dumps(base_event)

    elif error_type == "missing_event_type":
        base_event["event_type"] = ""
        return json.dumps(base_event)

    elif error_type == "missing_event_ts":
        base_event.pop("event_ts")
        return json.dumps(base_event)

    elif error_type == "missing_source":
        base_event["source"] = None
        return json.dumps(base_event)

    elif error_type == "missing_payload":
        base_event.pop("payload")
        return json.dumps(base_event)

    elif error_type == "invalid_event_type":
        base_event["event_type"] = "inventory.unknown_action"
        return json.dumps(base_event)

    elif error_type == "invalid_source":
        base_event["source"] = "external-api"
        return json.dumps(base_event)

    elif error_type == "bad_timestamp_format":
        base_event["event_ts"] = "2026/07/09 12:00:00"  # Invalid format (non-ISO)
        return json.dumps(base_event)

    elif error_type == "duplicate_id":
        base_event["event_id"] = random.choice(valid_event_ids)
        base_event["payload"]["current_stock_level"] = 9999
        return json.dumps(base_event)

    return json.dumps(base_event)


def main():
    parser = argparse.ArgumentParser(description="Simulate realistic retail day events and save to jsonl.")
    parser.add_argument("--events", type=int, default=1000, help="Total number of events to generate")
    parser.add_argument("--catalog", type=str, default=str(DEFAULT_CATALOG_PATH), help="Path to catalog.csv")
    parser.add_argument("--config", type=str, default=str(DEFAULT_PRODUCER_CONFIG_PATH), help="Path to producer.yml")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT_PATH), help="Path to write events JSONL")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)

    # 1. Load catalog SKUs
    catalog_path = Path(args.catalog)
    sku_list = load_catalog(catalog_path)

    # 2. Load producer config details (starting_stock, price)
    config_path = Path(args.config)
    producer_config = load_producer_config(config_path)
    sku_catalog_config = producer_config.get("sku_catalog", {})

    # Combine CSV data with config pricing/starting stock
    sku_details = {}
    for item in sku_list:
        sku_id = item["sku_id"]
        cfg = sku_catalog_config.get(sku_id, {})
        sku_details[sku_id] = {
            "sku_id": sku_id,
            "product_name": item["product_name"],
            "category": item["category"],
            "price": cfg.get("price", 9.99),
            "starting_stock": cfg.get("starting_stock", 100)
        }

    # 3. Simulate a 12-hour business day (08:00 to 20:00 UTC) yesterday
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    base_date = yesterday.replace(hour=8, minute=0, second=0, microsecond=0)

    # Relative traffic density weights by hour (0 to 11 offsets mapping to 08:00 to 19:00)
    hourly_weights = {
        0: 1.0,   # 08:00
        1: 1.2,   # 09:00
        2: 1.5,   # 10:00
        3: 2.0,   # 11:00
        4: 4.5,   # 12:00 (Lunch rush)
        5: 4.5,   # 13:00 (Lunch rush)
        6: 2.5,   # 14:00
        7: 2.0,   # 15:00
        8: 3.0,   # 16:00
        9: 5.5,   # 17:00 (Evening rush)
        10: 5.5,  # 18:00 (Evening rush)
        11: 3.0,  # 19:00
    }

    # Draw hourly distributions according to weights
    hours_drawn = random.choices(
        list(hourly_weights.keys()),
        weights=list(hourly_weights.values()),
        k=args.events
    )

    offsets = []
    for h in hours_drawn:
        sec = random.randint(0, 3599)
        offsets.append(h * 3600 + sec)

    # Sort offsets to process events chronologically
    offsets.sort()
    timestamps = [(base_date + timedelta(seconds=offset)).isoformat().replace("+00:00", "Z") for offset in offsets]

    # Initialize ledgers
    stock_levels = {sku_id: details["starting_stock"] for sku_id, details in sku_details.items()}
    pending_restocks = set()
    valid_event_ids = []
    events = []

    # Assign exactly 10% of indices as bad data
    bad_count = int(args.events * 0.10)
    bad_indices = set(random.sample(range(args.events), bad_count))

    # Sales weighting (make the first 10 SKUs 5x more popular to reflect realistic shopping dynamics)
    sku_ids = list(sku_details.keys())
    sku_weights = []
    for sku_id in sku_ids:
        try:
            num = int(sku_id.split("-")[1])
        except Exception:
            num = 1
        sku_weights.append(5.0 if num <= 10 else 1.0)

    # 4. Generate events
    for i in range(args.events):
        ts = timestamps[i]

        if i in bad_indices:
            # Generate invalid event
            event_str = generate_bad_event(ts, valid_event_ids, sku_ids, sku_details)
            events.append(event_str)
        else:
            # Generate valid event (restock vs checkout)
            should_restock = False
            restock_sku = None

            if pending_restocks:
                # Urgent restock if stock drops to or below 0
                urgent_skus = [sku for sku in pending_restocks if stock_levels[sku] <= 0]
                if urgent_skus:
                    restock_sku = random.choice(urgent_skus)
                    should_restock = True
                elif random.random() < 0.4:  # Interleave restocks organically
                    restock_sku = random.choice(list(pending_restocks))
                    should_restock = True

            if should_restock:
                # Restock event
                sku = restock_sku
                quantity_delta = random.randint(50, 100)
                stock_levels[sku] += quantity_delta
                pending_restocks.discard(sku)

                evt_id = str(uuid.uuid4())
                valid_event_ids.append(evt_id)

                event_obj = {
                    "event_id": evt_id,
                    "event_type": "inventory.stock_change",
                    "event_ts": ts,
                    "source": "streamflow-producer",
                    "entity_id": sku,
                    "payload": {
                        "sku_id": sku,
                        "quantity_delta": quantity_delta,
                        "current_stock_level": stock_levels[sku],
                        "checkout_lane": None,
                        "price": sku_details[sku]["price"]
                    }
                }
                events.append(json.dumps(event_obj))

            else:
                # Sale event
                # Choose SKU ensuring it has stock
                sku = None
                attempts = 0
                while attempts < 100:
                    candidate = random.choices(sku_ids, weights=sku_weights, k=1)[0]
                    if stock_levels[candidate] > 0:
                        sku = candidate
                        break
                    attempts += 1

                if sku is None:
                    # Fallback if selected SKUs are depleted
                    available = [s for s, stock in stock_levels.items() if stock > 0]
                    if available:
                        sku = random.choice(available)
                    else:
                        # Force restock if absolute depletion
                        sku = random.choice(sku_ids)
                        quantity_delta = 100
                        stock_levels[sku] += quantity_delta
                        evt_id = str(uuid.uuid4())
                        valid_event_ids.append(evt_id)
                        event_obj = {
                            "event_id": evt_id,
                            "event_type": "inventory.stock_change",
                            "event_ts": ts,
                            "source": "streamflow-producer",
                            "entity_id": sku,
                            "payload": {
                                "sku_id": sku,
                                "quantity_delta": quantity_delta,
                                "current_stock_level": stock_levels[sku],
                                "checkout_lane": None,
                                "price": sku_details[sku]["price"]
                            }
                        }
                        events.append(json.dumps(event_obj))
                        continue

                max_sale = min(5, stock_levels[sku])
                quantity_delta = -random.randint(1, max_sale)
                stock_levels[sku] += quantity_delta

                # Trigger restock if stock drops below 15
                if stock_levels[sku] < 15:
                    pending_restocks.add(sku)

                evt_id = str(uuid.uuid4())
                valid_event_ids.append(evt_id)
                checkout_lane = random.choice(CHECKOUT_LANES)

                event_obj = {
                    "event_id": evt_id,
                    "event_type": "inventory.stock_change",
                    "event_ts": ts,
                    "source": "streamflow-producer",
                    "entity_id": sku,
                    "payload": {
                        "sku_id": sku,
                        "quantity_delta": quantity_delta,
                        "current_stock_level": stock_levels[sku],
                        "checkout_lane": checkout_lane,
                        "price": sku_details[sku]["price"]
                    }
                }
                events.append(json.dumps(event_obj))

    # 5. Write to output file
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, mode="w", encoding="utf-8") as f:
        for ev in events:
            f.write(ev + "\n")

    print(f"Successfully generated {len(events)} events (90% valid, 10% bad).")
    print(f"Output saved to: {output_path}")


if __name__ == "__main__":
    main()
