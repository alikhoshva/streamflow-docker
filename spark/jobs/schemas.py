from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Inner payload definition
payload_schema = StructType([
    StructField("sku_id", StringType(), nullable=False),
    StructField("quantity_delta", IntegerType(), nullable=False),
    StructField("current_stock_level", IntegerType(), nullable=False),
    StructField("checkout_lane", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=False)
])

# Main outer wrapper contract
FINAL_RETAIL_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), nullable=True), # Nullable so we can parse events missing an ID
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", StringType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("entity_id", StringType(), nullable=False), 
    StructField("payload", payload_schema, nullable=False)
])
