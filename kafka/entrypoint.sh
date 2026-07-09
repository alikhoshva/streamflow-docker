#!/bin/bash
set -e

# Start a background process to create the topic once Kafka is healthy
(
  CONFIG_FILE="/opt/streamflow/config/pipeline.yml"
  
  echo "Waiting for Kafka broker to start..."
  until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    sleep 1
  done
  
  # Parse topic name from the yaml config
  if [ -f "$CONFIG_FILE" ]; then
      TOPIC=$(grep -A 2 'kafka:' "$CONFIG_FILE" | grep 'topic:' | awk '{print $2}' | tr -d '"' | tr -d "'")
  fi
  TOPIC=${TOPIC:-"streamflow.events"}
  
  echo "Creating Kafka topic: $TOPIC..."
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic "$TOPIC"
  echo "Kafka topic $TOPIC initialized."
) &

# Execute the default Kafka startup script
exec /etc/kafka/docker/run
