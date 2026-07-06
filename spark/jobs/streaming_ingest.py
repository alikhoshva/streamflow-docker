"""
Spark Streaming Ingestion Job.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from schemas import FINAL_RETAIL_SPARK_SCHEMA

def main():
    spark = SparkSession.builder \
        .appName("StreamFlowIngest") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
        .getOrCreate()

    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "streamflow.events") \
        .option("startingOffsets", "latest") \
        .load()
    string_df = raw_kafka_stream.selectExpr("CAST(value AS STRING) as json_string")

    # 4. Parse JSON string using your strict structural schema contract
    # (Assuming FINAL_RETAIL_SPARK_SCHEMA is defined)
    parsed_df = string_df.select(from_json(col("json_string"), FINAL_RETAIL_SPARK_SCHEMA).alias("data")).select("data.*")

    # Write the output stream to the console to print rows as they are received
    query = parsed_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
