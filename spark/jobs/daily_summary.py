"""
Spark Daily Summary Batch Job.
"""

from pyspark.sql import SparkSession


def main():

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("StreamflowDailySummary")
        .getOrCreate()
    )

    input_path = "/opt/streamflow/data/raw/smoke_test_output"

    output_path = "/opt/streamflow/data/curated/daily_summary"

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