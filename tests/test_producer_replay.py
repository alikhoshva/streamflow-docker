import json
import os
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from src.streamflow.producer import start_producer


def test_producer_replay_success(tmp_path):
    """Test that the producer loads, parses, and sends pre-generated events to Kafka."""
    # Create a temporary mock events file
    mock_events = [
        '{"event_id": "evt1", "entity_id": "SKU-1", "payload": {"sku_id": "SKU-1", "quantity_delta": -1}}',
        '{"event_id": "evt2", "entity_id": "SKU-2", "payload": {"sku_id": "SKU-2", "quantity_delta": 50}}',
        'invalid json record',
        '{"event_id": "evt3", "payload": {"sku_id": "SKU-3", "quantity_delta": -2}}'  # Missing entity_id, fallback to sku_id
    ]

    events_file = tmp_path / "mock_events.jsonl"
    with open(events_file, "w", encoding="utf-8") as f:
        for ev in mock_events:
            f.write(ev + "\n")

    # Mock configuration loading
    mock_config = {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "topic": "test.events"
        },
        "producer": {
            "events_per_second": 0,  # Zero sleep pacing to execute immediately
            "log_level": "DEBUG",
            "pregenerated_events_path": str(events_file)
        }
    }

    # Mock KafkaProducer
    mock_kafka_producer_class = MagicMock()
    mock_kafka_producer_instance = MagicMock()
    mock_kafka_producer_class.return_value = mock_kafka_producer_instance

    with patch("src.streamflow.producer._load_pipeline_config", return_value=mock_config), \
         patch("src.streamflow.producer.KafkaProducer", mock_kafka_producer_class):

        start_producer()

    # Verify KafkaProducer was initialized correctly
    mock_kafka_producer_class.assert_called_once()

    # Verify send calls
    assert mock_kafka_producer_instance.send.call_count == 4

    calls = mock_kafka_producer_instance.send.call_args_list

    # First call: evt1 (valid key)
    assert calls[0][0][0] == "test.events"
    assert calls[0][1]["key"] == "SKU-1"
    assert calls[0][1]["value"] == mock_events[0]

    # Second call: evt2 (valid key)
    assert calls[1][0][0] == "test.events"
    assert calls[1][1]["key"] == "SKU-2"
    assert calls[1][1]["value"] == mock_events[1]

    # Third call: invalid json record (key should be None)
    assert calls[2][0][0] == "test.events"
    assert calls[2][1]["key"] is None
    assert calls[2][1]["value"] == mock_events[2]

    # Fourth call: missing entity_id (fallback to sku_id key)
    assert calls[3][0][0] == "test.events"
    assert calls[3][1]["key"] == "SKU-3"
    assert calls[3][1]["value"] == mock_events[3]

    # Verify flush and close were called on finishing
    mock_kafka_producer_instance.flush.assert_called_once()
    mock_kafka_producer_instance.close.assert_called_once()
