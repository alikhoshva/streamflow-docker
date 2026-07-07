import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

events = [
    {
        "event_id": "evt_001",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "checkout_lane": "lane-1",
            "price": 12.99,
        },
    },
    {
        "event_id": "evt_002",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00002",
        "payload": {
            "sku_id": "SKU-00002",
            "quantity_delta": -2,
            "current_stock_level": 87,
            "checkout_lane": "lane-2",
            "price": 4.50,
        },
    },
    {
        "event_id": "evt_003",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00003",
        "payload": {
            "sku_id": "SKU-00003",
            "quantity_delta": 5,
            "current_stock_level": 205,
            "checkout_lane": None,
            "price": 19.99,
        },
    },
    {
        "event_id": "evt_004",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00004",
        "payload": {
            "sku_id": "SKU-00004",
            "quantity_delta": -1,
            "current_stock_level": 60,
            "checkout_lane": "self-checkout-1",
            "price": 7.25,
        },
    },
    {
        "event_id": "evt_005",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00005",
        "payload": {
            "sku_id": "SKU-00005",
            "quantity_delta": -3,
            "current_stock_level": 132,
            "checkout_lane": "lane-1",
            "price": 34.00,
        },
    },
    {
        "event_id": "evt_006",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00006",
        "payload": {
            "sku_id": "SKU-00006",
            "quantity_delta": -1,
            "current_stock_level": 18,
            "checkout_lane": "lane-3",
            "price": 99.99,
        },
    },
    {
        "event_id": "evt_007",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00007",
        "payload": {
            "sku_id": "SKU-00007",
            "quantity_delta": 10,
            "current_stock_level": 310,
            "checkout_lane": None,
            "price": 2.99,
        },
    },
    {
        "event_id": "evt_008",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00008",
        "payload": {
            "sku_id": "SKU-00008",
            "quantity_delta": -2,
            "current_stock_level": 44,
            "checkout_lane": "self-checkout-2",
            "price": 15.50,
        },
    },
    {
        "event_id": "evt_009",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00009",
        "payload": {
            "sku_id": "SKU-00009",
            "quantity_delta": -1,
            "current_stock_level": 76,
            "checkout_lane": "lane-4",
            "price": 8.75,
        },
    },
    {
        "event_id": "evt_010",
        "event_type": "inventory.stock_change",
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "source": "streamflow-producer",
        "entity_id": "SKU-00010",
        "payload": {
            "sku_id": "SKU-00010",
            "quantity_delta": -4,
            "current_stock_level": 121,
            "checkout_lane": "lane-2",
            "price": 27.30,
        },
    },
]

for event in events:
    producer.send("streamflow.events", event)
    print(f"sent {event['event_id']}")
    time.sleep(1)

producer.flush()
producer.close()
print("done")