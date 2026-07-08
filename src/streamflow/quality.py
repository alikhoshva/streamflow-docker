from pyspark.sql.functions import col, when, array, expr, lit, concat_ws, row_number, get_json_object
from pyspark.sql.window import Window

ALLOWED_EVENT_TYPES = ["inventory.stock_change"]
ALLOWED_SOURCES = ["streamflow-producer"]
ISO_TIMESTAMP_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}(:?\d{2})?)?$"


def validate_dataframe(df, existing_event_ids_df=None):
    """
    Validates a DataFrame of events containing a parsed 'data' struct and 'json_string'.
    Adds an 'errors' column (ArrayType(StringType)) with list of validation failure reasons.
    """
    # Define check for valid JSON document structure
    is_valid_json = col("json_string").isNotNull() & (col("json_string") != "") & get_json_object(col("json_string"), "$").isNotNull()

    # 1. JSON parsing check
    json_parse_err = when(~is_valid_json, "Failed to parse JSON").otherwise(None)

    # 2. Missing required fields checks
    event_id_err = when(
        is_valid_json & (col("data.event_id").isNull() | (col("data.event_id") == "")),
        "Missing event_id"
    ).otherwise(None)

    event_type_err = when(
        is_valid_json & (col("data.event_type").isNull() | (col("data.event_type") == "")),
        "Missing event_type"
    ).otherwise(None)

    event_ts_err = when(
        is_valid_json & (col("data.event_ts").isNull() | (col("data.event_ts") == "")),
        "Missing event_ts"
    ).otherwise(None)

    source_err = when(
        is_valid_json & (col("data.source").isNull() | (col("data.source") == "")),
        "Missing source"
    ).otherwise(None)

    payload_err = when(
        is_valid_json & col("data.payload").isNull(),
        "Missing payload"
    ).otherwise(None)

    # 3. Value constraints checks (Allowed lists)
    event_type_list_err = when(
        is_valid_json & col("data.event_type").isNotNull() & (col("data.event_type") != "") & (~col("data.event_type").isin(ALLOWED_EVENT_TYPES)),
        concat_ws(": ", lit("Invalid event_type"), col("data.event_type"))
    ).otherwise(None)

    source_list_err = when(
        is_valid_json & col("data.source").isNotNull() & (col("data.source") != "") & (~col("data.source").isin(ALLOWED_SOURCES)),
        concat_ws(": ", lit("Invalid source"), col("data.source"))
    ).otherwise(None)

    # 4. ISO Timestamp validation
    timestamp_err = when(
        is_valid_json & col("data.event_ts").isNotNull() & (col("data.event_ts") != "") & (~col("data.event_ts").rlike(ISO_TIMESTAMP_REGEX)),
        concat_ws(": ", lit("Invalid timestamp"), col("data.event_ts"))
    ).otherwise(None)

    # 5. In-batch duplicate check
    # Partition by event_id (only if event_id is valid/not-null/not-empty)
    # Order by ingest timestamp if available, otherwise by event_ts, otherwise static 1
    if "timestamp" in df.columns:
        order_col = col("timestamp").desc()
    elif "data.event_ts" in df.columns:
        order_col = col("data.event_ts").desc()
    else:
        order_col = lit(1)

    window_spec = Window.partitionBy("data.event_id").orderBy(order_col)
    row_num = row_number().over(window_spec)

    batch_duplicate_err = when(
        is_valid_json & col("data.event_id").isNotNull() & (col("data.event_id") != "") & (row_num > 1),
        "Duplicate event_id in batch"
    ).otherwise(None)

    # 6. Historical duplicate check
    if existing_event_ids_df is not None:
        existing_renamed = existing_event_ids_df.select(col("event_id").alias("existing_event_id"))
        df = df.join(existing_renamed, df["data.event_id"] == existing_renamed["existing_event_id"], "left")
        historical_duplicate_err = when(
            is_valid_json & col("existing_event_id").isNotNull(),
            "Duplicate event_id (already processed)"
        ).otherwise(None)
    else:
        # Placeholder column so array creation works
        df = df.withColumn("existing_event_id", lit(None).cast("string"))
        historical_duplicate_err = col("existing_event_id")


    # Combine all checks into an array
    errors_col = array(
        json_parse_err,
        event_id_err,
        event_type_err,
        event_ts_err,
        source_err,
        payload_err,
        event_type_list_err,
        source_list_err,
        timestamp_err,
        batch_duplicate_err,
        historical_duplicate_err
    )

    # Add errors column and remove null elements
    df = df.withColumn("errors", errors_col)
    df = df.withColumn("errors", expr("filter(errors, x -> x is not null)"))

    # Clean up the temp join column
    df = df.drop("existing_event_id")

    return df


def validate_record(record):
    """
    Fallback validate_record function for compatibility.
    """
    return True

