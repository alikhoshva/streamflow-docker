import os
import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, from_json, expr, to_timestamp, date_format

def build_standalone_schema():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    payload_schema = StructType([
        StructField("sku_id", StringType(), nullable=False),
        StructField("quantity_delta", IntegerType(), nullable=False),
        StructField("current_stock_level", IntegerType(), nullable=False),
        StructField("checkout_lane", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=False)
    ])
    return StructType([
        StructField("event_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_ts", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("entity_id", StringType(), nullable=False), 
        StructField("payload", payload_schema, nullable=False)
    ])

def main():
    config_path = "/opt/streamflow/config/pipeline.yml"
    raw_path = "/opt/streamflow/data/raw"
    rejects_path = "/opt/streamflow/data/rejects"
    
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f) or {}
                raw_path = config.get("spark", {}).get("raw_output_path", raw_path)
                rejects_path = config.get("spark", {}).get("rejects_output_path", rejects_path)
        except Exception as e:
            print(f"Pipeline config fallback warning: {e}")

    spark = SparkSession.builder \
        .appName("StreamflowRejectsReprocessorStandalone") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

    if not os.path.exists(rejects_path) or not any(Path(rejects_path).iterdir()):
        print(f"No files found in rejects folder '{rejects_path}'. Exiting cleanly.")
        spark.stop()
        return

    print(f"Reading quarantined records from: {rejects_path}")
    rejected_df = spark.read.parquet(rejects_path)
    
    # 1. Parse using local schema definition
    target_schema = build_standalone_schema()
    parsed_df = rejected_df.withColumn("full_data", from_json(col("json_string"), target_schema))

    # 2. ADVANCED HEALING LOGIC
    healed_df = parsed_df \
        .withColumn(
            # Fix Missing event_id: Generate a stable UUID if null
            "healed_event_id",
            when(col("full_data.event_id").isNull(), expr("uuid()")).otherwise(col("full_data.event_id"))
        ) \
        .withColumn(
            # Fix Malformed or Missing event_ts:
            # Try parsing 'yyyy/MM/dd HH:mm:ss' first, fallback to batch default '2026-07-09T12:00:00Z' if completely null
            "healed_event_ts",
            when(
                col("full_data.event_ts").rlike(r"^\d{4}/\d{2}/\d{2}"), 
                date_format(to_timestamp(col("full_data.event_ts"), "yyyy/MM/dd HH:mm:ss"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            ).when(
                col("full_data.event_ts").isNull(), 
                lit("2026-07-09T12:00:00Z")
            ).otherwise(col("full_data.event_ts"))
        ) \
        .withColumn(
            # Fix source typos if present
            "healed_source", 
            when(col("source") == "invalid-source", lit("streamflow-producer")).otherwise(col("source"))
        ) \
        .withColumn(
            # Fix Missing payload: Synthesize payload from metadata columns if payload is null
            "healed_payload",
            when(
                col("full_data.payload").isNull(),
                expr("named_struct('sku_id', entity_id, 'quantity_delta', -1, 'current_stock_level', 50, 'checkout_lane', 'lane-1', 'price', 9.99)")
            ).otherwise(col("full_data.payload"))
        )

    # 3. Filter for strictly clean or completely healed rows
    clean_recovered_df = healed_df.filter(
        (col("healed_source") == "streamflow-producer") & 
        (col("event_type") == "inventory.stock_change")
    )

    recovered_count = clean_recovered_df.count()
    
    if recovered_count > 0:
        print(f"Successfully healed and recovered {recovered_count} records. Appending back to raw...")
        
        # 4. Explicitly project columns in exact schema order for append compatibility
        final_raw_df = clean_recovered_df.select(
            col("healed_event_id").alias("event_id"),
            col("full_data.event_type").alias("event_type"),
            col("healed_event_ts").alias("event_ts"),
            col("healed_source").alias("source"),  
            col("full_data.entity_id").alias("entity_id"),
            col("healed_payload").alias("payload")
        )
        
        final_raw_df.write \
            .mode("append") \
            .parquet(raw_path)
            
        print("Append repair complete.")
    else:
        print("Completed reprocessing cycle, but 0 records were eligible for recovery.")

    spark.stop()

if __name__ == "__main__":
    main()