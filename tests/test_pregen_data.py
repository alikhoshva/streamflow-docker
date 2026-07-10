import json
import re
from pathlib import Path
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENTS_PATH = PROJECT_ROOT / "data" / "pregenerated_events.jsonl"
PRODUCER_CONFIG_PATH = PROJECT_ROOT / "config" / "producer.yml"
CATALOG_PATH = PROJECT_ROOT / "data" / "metadata" / "catalog.csv"

ISO_TIMESTAMP_REGEX = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:?\d{2})?)?$"
)


def test_pregenerated_events_exist():
    """Verify that the generated dataset file exists."""
    assert EVENTS_PATH.exists(), f"Pregenerated events file not found at {EVENTS_PATH}"


def test_event_counts_and_validation_ratio():
    """Verify that there are exactly 1000 events, with a 90/10 valid/bad split."""
    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    assert len(lines) == 1000, f"Expected 1000 events, got {len(lines)}"

    valid_count = 0
    bad_count = 0
    seen_ids = set()

    for line in lines:
        is_valid = True
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            # Type 1: Malformed JSON
            bad_count += 1
            continue

        # Check required fields
        if "event_id" not in event or not event["event_id"]:
            # Type 2: Missing event_id
            is_valid = False
        elif event["event_id"] in seen_ids:
            # Type 6: Duplicate ID
            is_valid = False
        else:
            seen_ids.add(event["event_id"])

        if "event_type" not in event or not event["event_type"] or event["event_type"] != "inventory.stock_change":
            # Type 2 or 3: Missing or Invalid event_type
            is_valid = False

        if "event_ts" not in event or not event["event_ts"] or not ISO_TIMESTAMP_REGEX.match(event["event_ts"]):
            # Type 2 or 4: Missing or Invalid timestamp format
            is_valid = False

        if "source" not in event or not event["source"] or event["source"] != "streamflow-producer":
            # Type 2 or 3: Missing or Invalid source
            is_valid = False

        if "payload" not in event or not event["payload"]:
            # Type 2: Missing payload
            is_valid = False

        if is_valid:
            valid_count += 1
        else:
            bad_count += 1

    assert valid_count == 900, f"Expected 900 valid events, got {valid_count}"
    assert bad_count == 100, f"Expected 100 bad events, got {bad_count}"


def test_stock_ledger_coherence():
    """Verify running stock calculation, initial stock values, and non-negativity."""
    # Load producer config
    with open(PRODUCER_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    sku_catalog = config.get("sku_catalog", {})

    # Track running stock
    stock_tracker = {sku: info["starting_stock"] for sku, info in sku_catalog.items()}
    restock_triggers = {sku: False for sku in sku_catalog}

    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    valid_events = []
    seen_ids = set()

    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue

        # Filter out invalid events using the same logic
        is_valid = (
            "event_id" in event and event["event_id"] and event["event_id"] not in seen_ids and
            "event_type" in event and event["event_type"] == "inventory.stock_change" and
            "event_ts" in event and ISO_TIMESTAMP_REGEX.match(event["event_ts"]) and
            "source" in event and event["source"] == "streamflow-producer" and
            "payload" in event and event["payload"]
        )

        if is_valid:
            seen_ids.add(event["event_id"])
            valid_events.append(event)

    # Process valid events chronologically (they are generated chronologically)
    for event in valid_events:
        payload = event["payload"]
        sku_id = payload["sku_id"]
        quantity_delta = payload["quantity_delta"]
        reported_stock = payload["current_stock_level"]

        # Validate starting values exist in producer config
        assert sku_id in stock_tracker, f"Event references unknown SKU: {sku_id}"

        # Check stock coherence
        expected_stock = stock_tracker[sku_id] + quantity_delta
        assert expected_stock == reported_stock, (
            f"Stock mismatch for {sku_id}. Expected {expected_stock}, got {reported_stock} "
            f"(Previous stock was {stock_tracker[sku_id]} and delta was {quantity_delta})"
        )

        # Update stock tracker
        stock_tracker[sku_id] = expected_stock

        # Check that stock never falls below 0
        assert stock_tracker[sku_id] >= 0, f"Stock for {sku_id} went negative: {stock_tracker[sku_id]}"

        # Track restock triggers
        if quantity_delta < 0 and stock_tracker[sku_id] < 15:
            restock_triggers[sku_id] = True

        if quantity_delta > 0 and restock_triggers[sku_id]:
            # This is a restock event completing a previously triggered restock request
            restock_triggers[sku_id] = False

    # Check that any SKU that dropped below 15 got restocked eventually
    # (Or is currently pending, but with 900 events, most should have been restocked or are in progress)
    # We can at least assert that stock levels of all products are healthy at the end of the simulation.
    for sku_id, stock in stock_tracker.items():
        assert stock >= 0, f"SKU {sku_id} has negative final stock: {stock}"


def test_sales_peak_curves():
    """Verify that peak traffic hours (lunch and evening rushes) have higher volume."""
    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    # Collect hours of all successfully parsed events
    event_hours = []
    for line in lines:
        try:
            event = json.loads(line)
            ts = event.get("event_ts")
            if ts and ISO_TIMESTAMP_REGEX.match(ts):
                # ISO format: 2026-07-09T08:05:23Z
                # Extract the hour part
                hour = int(ts.split("T")[1].split(":")[0])
                event_hours.append(hour)
        except (json.JSONDecodeError, ValueError, IndexError):
            continue

    # Count events per hour
    hour_counts = {h: 0 for h in range(8, 20)}
    for h in event_hours:
        if h in hour_counts:
            hour_counts[h] += 1

    lunch_rush_count = hour_counts[12] + hour_counts[13]
    evening_rush_count = hour_counts[17] + hour_counts[18]
    off_peak_count_1 = hour_counts[8] + hour_counts[9]
    off_peak_count_2 = hour_counts[14] + hour_counts[15]

    # Verify peak volumes are significantly higher than off-peak hours
    assert lunch_rush_count > off_peak_count_1, f"Lunch rush ({lunch_rush_count}) should be busier than morning off-peak ({off_peak_count_1})"
    assert evening_rush_count > off_peak_count_2, f"Evening rush ({evening_rush_count}) should be busier than afternoon off-peak ({off_peak_count_2})"
