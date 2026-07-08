import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

from src.streamflow.schemas import RETAIL_SPARK_SCHEMA
from src.streamflow.quality import validate_dataframe


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("StreamflowQualityTests") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()


def test_validate_dataframe_happy_path(spark):
    # Valid JSON string conforming to the schema
    valid_json = """{
        "event_id": "evt_001",
        "event_type": "inventory.stock_change",
        "event_ts": "2026-07-08T13:30:00Z",
        "source": "streamflow-producer",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "price": 12.99
        }
    }"""
    df = spark.createDataFrame([(valid_json,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.collect()

    assert len(results) == 1
    assert len(results[0]["errors"]) == 0


def test_validate_dataframe_json_parsing_failed(spark):
    invalid_json = "{malformed json"
    df = spark.createDataFrame([(invalid_json,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.collect()

    assert len(results) == 1
    assert "Failed to parse JSON" in results[0]["errors"]


def test_validate_dataframe_missing_required_fields(spark):
    # Missing event_id, event_type, and payload
    invalid_json = """{
        "event_ts": "2026-07-08T13:30:00Z",
        "source": "streamflow-producer",
        "entity_id": "SKU-00001"
    }"""
    df = spark.createDataFrame([(invalid_json,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.collect()

    assert len(results) == 1
    errors = results[0]["errors"]
    assert "Missing event_id" in errors
    assert "Missing event_type" in errors
    assert "Missing payload" in errors


def test_validate_dataframe_value_constraints(spark):
    # Invalid event_type and invalid source
    invalid_json = """{
        "event_id": "evt_002",
        "event_type": "invalid.event_type",
        "event_ts": "2026-07-08T13:30:00Z",
        "source": "invalid-source",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "price": 12.99
        }
    }"""
    df = spark.createDataFrame([(invalid_json,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.collect()

    assert len(results) == 1
    errors = results[0]["errors"]
    assert "Invalid event_type: invalid.event_type" in errors
    assert "Invalid source: invalid-source" in errors


def test_validate_dataframe_invalid_timestamp(spark):
    invalid_json = """{
        "event_id": "evt_003",
        "event_type": "inventory.stock_change",
        "event_ts": "not-a-timestamp",
        "source": "streamflow-producer",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "price": 12.99
        }
    }"""
    df = spark.createDataFrame([(invalid_json,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.collect()

    assert len(results) == 1
    errors = results[0]["errors"]
    assert "Invalid timestamp: not-a-timestamp" in errors


def test_validate_dataframe_in_batch_duplicates(spark):
    # Two identical records with same event_id
    json1 = """{
        "event_id": "evt_dup",
        "event_type": "inventory.stock_change",
        "event_ts": "2026-07-08T13:30:00Z",
        "source": "streamflow-producer",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "price": 12.99
        }
    }"""
    
    # Create DataFrame with both events and timestamps for ordering
    df = spark.createDataFrame([
        (json1, 100),
        (json1, 200)  # Later timestamp, row_num=1
    ], ["json_string", "timestamp"])

    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string"),
        col("timestamp")
    )

    validated_df = validate_dataframe(parsed_df)
    results = validated_df.orderBy("timestamp").collect()

    assert len(results) == 2
    # The first one (timestamp 100) should be row_num=2 (so it is flagged as duplicate)
    assert "Duplicate event_id in batch" in results[0]["errors"]
    # The second one (timestamp 200) should be row_num=1 (so no duplicate error)
    assert len(results[1]["errors"]) == 0


def test_validate_dataframe_historical_duplicates(spark):
    json1 = """{
        "event_id": "evt_hist_dup",
        "event_type": "inventory.stock_change",
        "event_ts": "2026-07-08T13:30:00Z",
        "source": "streamflow-producer",
        "entity_id": "SKU-00001",
        "payload": {
            "sku_id": "SKU-00001",
            "quantity_delta": -1,
            "current_stock_level": 149,
            "price": 12.99
        }
    }"""
    df = spark.createDataFrame([(json1,)], ["json_string"])
    parsed_df = df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string")
    )

    # Mock historical event_ids DataFrame
    existing_df = spark.createDataFrame([("evt_hist_dup",)], ["event_id"])

    validated_df = validate_dataframe(parsed_df, existing_df)
    results = validated_df.collect()

    assert len(results) == 1
    assert "Duplicate event_id (already processed)" in results[0]["errors"]
