# Streamflow: Containerized Stream Processing

This project is a containerized stream processing pipeline that ingests data via Kafka, processes/analyzes it in real-time with Apache Spark, and orchestrates workflows using Apache Airflow.

## Folder Structure

```
.
├── airflow/
│   └── dags/
│       └── streamflow_daily_summary.py   # Airflow DAG for orchestration
├── data/
│   ├── raw/                              # Raw input data directory
│   ├── curated/                          # Curated/processed data
│   ├── rejects/                          # Failed/rejected records
│   └── checkpoints/                      # Spark checkpoints for streaming
├── docker/
│   ├── airflow.Dockerfile                # Custom Airflow container
│   ├── compose.yml                       # Docker Compose multi-container setup
│   └── producer.Dockerfile               # Custom Python Kafka producer container
├── kafka/                                # Kafka configurations/scripts
├── spark/
│   └── jobs/
│       ├── streaming_ingest.py           # Real-time Spark streaming ingestion job
│       └── daily_summary.py              # Daily batch summarization job
├── src/
│   └── streamflow/
│       ├── __init__.py                   # Package initialization
│       ├── producer.py                   # Custom Kafka message producer logic
│       ├── schemas.py                    # Schemas definitions for ingestion/quality
│       └── quality.py                    # Data quality / validation checks
├── tests/                                # Test suites
└── README.md                             # Project documentation
```
