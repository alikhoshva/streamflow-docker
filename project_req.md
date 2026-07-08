# StreamFlow Phase 1 Requirements

## 1. Infrastructure & Docker
- Run broker (Kafka/Redpanda), Airflow, Spark, and Producer locally via `docker/compose.yml`.
- Externalize broker addresses, topic names, and file paths using environment variables or config files (no hardcoding).

## 2. Event Producer (`src/streamflow/producer.py`)
- Generate synthetic events for a chosen domain (e.g., e-commerce, telemetry, logs).
- Publish JSON events to a configured Kafka/Redpanda topic (e.g., `streamflow.events`).

## 3. Event Schema & Validation
- Require fields: `event_id` (str), `event_type` (str), `event_ts` (ISO timestamp), `source` (str), `payload` (JSON obj).
- Optional field: `entity_id` (str).
- Validate timestamps, check `event_type` and `source` against allowed lists, and detect duplicate `event_id`s.

## 4. Spark Structured Streaming (`spark/jobs/streaming_ingest.py`)
- Read events incrementally from the Kafka/Redpanda topic.
- Parse and validate incoming JSON payloads against the event schema.
- Write valid raw records to Parquet files in `data/raw/`.
- Write invalid/failed records and rejection reasons to `data/rejects/`.
- Maintain streaming progress using a checkpoint directory in `data/checkpoints/`.

## 5. Spark Batch Summary (`spark/jobs/daily_summary.py`)
- Read persisted raw Parquet files from `data/raw/`.
- Aggregate domain metrics (e.g., event counts by type, source breakdowns, time windows).
- Write curated analytics summaries to `data/curated/daily_summary`.

## 6. Airflow Orchestration (`airflow/dags/streamflow_daily_summary.py`)
- Coordinate bounded workflows only (no infinite streaming loops).
- Trigger or schedule the Spark batch summary job.
- Validate output file existence and record execution metadata/logs.

## 7. Logging & Observability
- Emit structured logs with start/end timestamps, run IDs, broker/topic configs, and file paths.
- Log exact record counts: total input rows, valid records passed, and rejected records.

## 8. Required Deliverables & Structure
- `docker/compose.yml`, `docker/producer.Dockerfile`, `docker/airflow.Dockerfile`
- `src/streamflow/producer.py`, `src/streamflow/schemas.py`, `src/streamflow/quality.py`
- `spark/jobs/streaming_ingest.py`, `spark/jobs/daily_summary.py`
- `airflow/dags/streamflow_daily_summary.py`
- `data/` directory mounts (`raw/`, `curated/`, `rejects/`, `checkpoints/`)
- Unit & smoke tests in `tests/` verifying schema validation, data transformations, and pipeline execution.
- `README.md` with clear setup, execution, and troubleshooting instructions.
