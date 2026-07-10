import os
import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Resolve project paths for configuration loading
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

def main():
    # Load central configuration file to get production paths
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
            print(f"Warning: Failed to load pipeline configuration: {e}")

    # Initialize Spark Session for the batch recovery run
    spark = SparkSession.builder \
        .appName("StreamflowRejectsReprocessorBatch") \
        .getOrCreate()

    # Safeguard: Check if the rejects directory exists and has files
    if not os.path.exists(rejects_path) or not any(Path(rejects_path).iterdir()):
        print(f"No rejected records found in '{rejects_path}'. Exiting cleanly.")
        spark.stop()
        return

    print(f"Reading quarantined records from: {rejects_path}")
    rejected_df = spark.read.parquet(rejects_path)
    
    total_rejects = rejected_df.count()
    if total_rejects == 0:
        print("Rejects directory contained metadata but 0 records. Exiting.")
        spark.stop()
        return

    # --- CORRECTION AND CLEANUP LOGIC ---
    # Fix known structural mistakes (e.g., mapping 'invalid-source' back to allowed lists)
    corrected_df = rejected_df.withColumn(
        "source", 
        when(col("source") == "invalid-source", lit("streamflow-producer")).otherwise(col("source"))
    )

    # Filter for records that are now fully compliant with validation rules
    clean_recovered_df = corrected_df.filter(
        (col("source") == "streamflow-producer") & 
        (col("event_type") == "inventory.stock_change")
    )

    recovered_count = clean_recovered_df.count()
    
    if recovered_count > 0:
        print(f"Successfully healed {recovered_count} of {total_rejects} records. Re-injecting into raw...")
        
        # Match the exact schema structure of data/raw/
        final_raw_df = clean_recovered_df.select(
            "event_id", "event_type", "event_ts", "source", "entity_id"
        )
        
        # Safe append back into the primary raw analytics storage lake
        final_raw_df.write \
            .mode("append") \
            .parquet(raw_path)
            
        print("Re-injection complete.")
    else:
        print("Batch processing completed, but no records could be successfully healed.")

    spark.stop()

if __name__ == "__main__":
    main()