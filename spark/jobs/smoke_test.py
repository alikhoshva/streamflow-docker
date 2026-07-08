import yaml
from pyspark.sql import SparkSession

# 1. Load your central configuration
with open("/opt/streamflow/config/pipeline.yml", "r") as f:
    config = yaml.safe_load(f)

raw_path = config["spark"]["raw_output_path"]

# 2. Initialize the Spark environment mapped in your Docker container
spark = SparkSession.builder \
    .appName(config["spark"]["app_name"]) \
    .getOrCreate()

# 3. Create a minimal 1-row dataframe matching your specific contract format
data = [("evt_000", "prototype_test", "store_austin_04")]
columns = ["event_id", "event_type", "entity_id"]

df = spark.createDataFrame(data, columns)

# 4. Attempt to write to your shared data lake storage volume
df.write.mode("overwrite").parquet(f"{raw_path}/smoke_test_output")

print("SUCCESS: Connection pieces are wired correctly!")
spark.stop()