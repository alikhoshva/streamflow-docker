# Streamflow: Containerized Stream Processing & Analytics Pipeline

Streamflow is an end-to-end containerized stream processing, data quality, and analytics pipeline. It ingests retail sales events in real-time via Apache Kafka, processes and validates data with Apache Spark, orchestrates workflows using Apache Airflow 3, and visualizes insights on an interactive Streamlit dashboard.

---

## 🏗️ Pipeline Architecture

```
[Kafka Producer] ---> (Kafka Topic: retail_events) ---> [Spark Streaming Ingestion]
                                                                  |
                                              +-------------------+-------------------+
                                              |                                       |
                                     (Valid Records)                          (Invalid Records)
                                              v                                       v
                                     `data/raw/`                            `data/rejects/`
                                              |                                       |
                                    [Spark Batch Summary]                     [Reprocess Recovery]
                                              v
                                   `data/curated/daily_summary/`
                                              |
                                     [Streamlit Dashboard]
```

- **Messaging**: Apache Kafka 4.3.1 (Dual listener: `localhost:9092` host, `kafka:29092` Docker network).
- **Streaming Ingestion**: Real-time PySpark streaming ingestion (`spark/jobs/streaming_ingest.py`) enforcing schema validation (`src/streamflow/schemas.py`) and quality checks (`src/streamflow/quality.py`).
- **Batch Processing**: PySpark batch job (`spark/jobs/daily_summary.py`) generating daily sales summaries, top-selling items, and low-stock inventory alerts in Parquet format.
- **Data Recovery**: Spark reprocessing job (`spark/jobs/reprocess_rejects.py`) to re-evaluate quarantined bad records.
- **Orchestration**: Apache Airflow 3 (`docker/airflow.Dockerfile`) managing automated DAGs in `airflow/dags/`.
- **Analytics UI**: Interactive Streamlit dashboard (`scripts/streamlit_dashboard.py`).

---

## 📁 Repository Structure

```
.
├── airflow/
│   └── dags/
│       ├── streamflow_daily_summary.py       # Airflow DAG for daily summary batch jobs
│       └── streamflow_reprocess_recovery.py  # Airflow DAG for invalid data recovery
├── data/
│   ├── raw/                                  # Valid raw JSON streaming data
│   ├── curated/                              # Aggregated sales, top items, & alerts
│   ├── rejects/                              # Quarantined bad records
│   ├── metadata/                             # Catalog metadata (SKU details)
│   └── checkpoints/                          # Spark Streaming state checkpoints
├── docker/
│   ├── compose.yml                           # Docker Compose multi-container orchestrator
│   ├── airflow.Dockerfile                    # Custom Airflow 3 container
│   └── producer.Dockerfile                   # Python Kafka event producer container
├── kafka/                                    # Kafka entrypoint & healthcheck scripts
├── scripts/
│   ├── streamlit_dashboard.py                # Analytics UI (Streamlit)
│   ├── generate_pregen_data.py               # Mock sales event & catalog generator
│   └── cleanup_data.py                       # Pipeline data purge utility
├── spark/
│   └── jobs/
│       ├── streaming_ingest.py               # Real-time PySpark Kafka ingestion
│       ├── daily_summary.py                  # Batch daily summarization & alerts
│       ├── reprocess_rejects.py              # Failed record recovery job
│       └── smoke_test.py                     # Spark connectivity test
├── src/
│   └── streamflow/
│       ├── producer.py                       # Event generator & Kafka producer logic
│       ├── schemas.py                        # PySpark & StructType schemas
│       └── quality.py                        # Data quality & validation rules
├── tests/                                    # Pytest test suite
├── requirements.in                           # Top-level direct dependencies
├── requirements.txt                          # Compiled deterministic lockfile
├── pyproject.toml                            # Project metadata & dependency definitions
├── devenv.nix                                # Devenv environment configuration
├── Makefile                                  # Task automation targets
└── README.md                                 # Project documentation
```

---

## 🚀 Quick Start

### 1. Start Pipeline Services
Start Kafka, the event producer, Spark, and Airflow containers:
```bash
make up
```

### 2. Access Web Dashboards
- **Airflow Web UI**: [http://localhost:8080](http://localhost:8080) *(Credentials: `admin` / `admin`)*
- **Streamlit Analytics Dashboard**: [http://localhost:8501](http://localhost:8501) *(Run `make dashboard`)*
- **Spark Job UI**: [http://localhost:4040](http://localhost:4040) *(Active during Spark executions)*

### 3. Launch Analytics Dashboard
Launch the Streamlit analytics interface:
```bash
make dashboard
```

---

## 🛠️ Dependency Management (`pip-tools`)

Dependencies are managed strictly with **`pip-tools`** to prevent version drift.

- **Edit `requirements.in`**: Add or update direct dependencies here.
- **Recompile Lockfile**:
  ```bash
  make compile-deps
  ```
- **Sync Environment**:
  ```bash
  make sync-deps
  ```

> **Note**: Do not edit `requirements.txt` directly. Always update `requirements.in` and run `make compile-deps`.

---

## 🧪 Testing & Data Cleanup

Run the automated test suite (cleans pipeline data directories before executing tests):
```bash
make test
```

Purge generated pipeline data (`raw/`, `curated/`, `rejects/`, `checkpoints/`):
```bash
make clean
```

Stop Docker services and clean data:
```bash
make down
```

---

## 📑 Summary of Makefile Commands

| Command | Action |
| :--- | :--- |
| `make up` | Start all Docker Compose containers. |
| `make down` | Stop containers and purge temporary pipeline data. |
| `make clean` | Purge data subdirectories (`raw`, `curated`, `rejects`, `checkpoints`). |
| `make test` | Clean data and run Pytest suite (`pytest tests/`). |
| `make dashboard` | Launch the Streamlit analytics UI. |
| `make compile-deps` | Recompile `requirements.txt` from `requirements.in` using `pip-compile`. |
| `make sync-deps` | Sync Python environment with `requirements.txt` using `pip-sync`. |
| `make logs` | Stream real-time logs from Docker containers. |
| `make slides` | Compile LaTeX slides presentation inside Docker. |
