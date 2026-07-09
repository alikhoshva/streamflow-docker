"""
Spark Daily Summary Batch Job.
"""

import os
import yaml
from pathlib import Path
from pyspark.sql import SparkSession


def main():
    # Load configuration
    project_root = Path(__file__).resolve().parent.parent.parent
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

    app_name = config.get("spark", {}).get("app_name", "StreamflowDailySummary")
    raw_path = config.get("spark", {}).get("raw_output_path", "/opt/streamflow/data/raw")
    curated_path = config.get("spark", {}).get("curated_output_path", "/opt/streamflow/data/curated")

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    input_path = raw_path

    output_path = f"{curated_path}/daily_summary"

    # Read raw parquet
    df = spark.read.parquet(input_path)

    print("Input Schema:")
    df.printSchema()

    print("Input Data:")
    df.show(truncate=False)


    # Create top_items output
    top_items = (
        df.select(
            "event_id",
            "event_type",
            "entity_id"
        )
    )

    top_items.write \
        .mode("overwrite") \
        .parquet(
            f"{output_path}/top_items"
        )


    # Create low_stock_alerts output
    low_stock_alerts = (
        df.select(
            "event_id",
            "event_type",
            "entity_id"
        )
    )

    low_stock_alerts.write \
        .mode("overwrite") \
        .parquet(
            f"{output_path}/low_stock_alerts"
        )


    spark.stop()


if __name__ == "__main__":
    main()