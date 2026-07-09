import sys
import os
import uuid
import yaml
from datetime import datetime
from pathlib import Path
import structlog

# Add project root and src to sys.path to resolve streamflow package imports
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, size, concat_ws
from streamflow.schemas import RETAIL_SPARK_SCHEMA
from streamflow.quality import validate_dataframe

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()


def main():
    run_id = str(uuid.uuid4())

    # Try to load pipeline configuration
    config_paths = [
        "/opt/streamflow/config/pipeline.yml",
        project_root / "config" / "pipeline.yml",
        "config/pipeline.yml"
    ]
    config = {}
    for path in config_paths:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    config = yaml.safe_load(f) or {}
                break
            except Exception:
                pass

    # Externalize configs with environment variables or pipeline.yml values
    app_name = os.environ.get("SPARK_APP_NAME", config.get("spark", {}).get("app_name", "StreamFlowIngest"))
    broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", config.get("kafka", {}).get("bootstrap_servers", "localhost:9092"))
    topic = os.environ.get("KAFKA_TOPIC", config.get("kafka", {}).get("topic", "streamflow.events"))
    raw_path = os.environ.get("RAW_OUTPUT_PATH", config.get("spark", {}).get("raw_output_path", "data/raw"))
    rejects_path = os.environ.get("REJECTS_OUTPUT_PATH", config.get("spark", {}).get("rejects_output_path", "data/rejects"))
    checkpoint_location = os.environ.get("CHECKPOINT_LOCATION", config.get("spark", {}).get("checkpoint_location", "data/checkpoints/raw"))

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Capture raw JSON string and Kafka ingest timestamp for duplicate validation
    string_df = raw_kafka_stream.selectExpr("CAST(value AS STRING) as json_string", "timestamp")

    # Parse JSON string using RETAIL_SPARK_SCHEMA
    parsed_df = string_df.select(
        from_json(col("json_string"), RETAIL_SPARK_SCHEMA).alias("data"),
        col("json_string"),
        col("timestamp")
    )

    def process_batch(batch_df, batch_id):
        start_time = datetime.utcnow().isoformat()

        # Check for historical duplicates by reading raw_path if it contains data
        existing_event_ids_df = None
        if os.path.exists(raw_path) and any(Path(raw_path).iterdir()):
            try:
                existing_event_ids_df = spark.read.parquet(raw_path).select("event_id").distinct()
            except Exception:
                pass

        # Validate batch
        validated_df = validate_dataframe(batch_df, existing_event_ids_df)

        total_count = batch_df.count()
        if total_count == 0:
            return

        valid_df = validated_df.filter(size(col("errors")) == 0).select("data.*")
        invalid_df = validated_df.filter(size(col("errors")) > 0).select(
            col("json_string"),
            concat_ws("; ", col("errors")).alias("rejection_reason"),
            col("data.event_id"),
            col("data.event_type"),
            col("data.event_ts"),
            col("data.source"),
            col("data.entity_id")
        )

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        # Write to raw / rejects
        if valid_count > 0:
            valid_df.write \
                .mode("append") \
                .parquet(raw_path)

        if invalid_count > 0:
            invalid_df.write \
                .mode("append") \
                .parquet(rejects_path)

        end_time = datetime.utcnow().isoformat()

        # Log metrics
        logger.info(
            "Processed stream micro-batch",
            batch_id=batch_id,
            run_id=run_id,
            start_ts=start_time,
            end_ts=end_time,
            broker=broker,
            topic=topic,
            raw_path=raw_path,
            rejects_path=rejects_path,
            total_records=total_count,
            valid_records=valid_count,
            rejected_records=invalid_count
        )

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()

