import os
import yaml
from pathlib import Path
from pyspark.sql import SparkSession

# 1. Load your central configuration
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

app_name = config.get("spark", {}).get("app_name", "PrototypeSmokeTest")
raw_path = config.get("spark", {}).get("raw_output_path", "data/raw")

# 2. Initialize the Spark environment mapped in your Docker container
spark = SparkSession.builder \
    .appName(app_name) \
    .getOrCreate()

# 3. Create a minimal 1-row dataframe matching your specific contract format
data = [("evt_000", "prototype_test", "store_austin_04")]
columns = ["event_id", "event_type", "entity_id"]

df = spark.createDataFrame(data, columns)

# 4. Attempt to write to your shared data lake storage volume
df.write.mode("overwrite").parquet(f"{raw_path}/smoke_test_output")

print("SUCCESS: Connection pieces are wired correctly!")
spark.stop()