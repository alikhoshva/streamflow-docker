# streamflow-docker

file structure:

p1-streamflow-containerized-stream-processing/
  airflow/
    dags/
      streamflow_daily_summary.py
  docker/
    airflow.Dockerfile
    compose.yml
    producer.Dockerfile
  kafka/
  spark/
    jobs/
      streaming_ingest.py
      daily_summary.py
  src/
    streamflow/
      __init__.py
      producer.py
      schemas.py
      quality.py
  data/
    raw/
    curated/
    rejects/
    checkpoints/
  tests/
  README.md