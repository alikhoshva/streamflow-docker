#!/bin/bash
set -e

CONFIG_FILE="/opt/streamflow/config/pipeline.yml"

# Parse bootstrap servers and topic name from the yaml config
if [ -f "$CONFIG_FILE" ]; then
    BOOTSTRAP_SERVERS=$(grep -A 2 'kafka:' "$CONFIG_FILE" | grep 'bootstrap_servers:' | awk '{print $2}' | tr -d '"' | tr -d "'")
    TOPIC=$(grep -A 2 'kafka:' "$CONFIG_FILE" | grep 'topic:' | awk '{print $2}' | tr -d '"' | tr -d "'")
fi

# Fallback values
BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-"kafka:29092"}
TOPIC=${TOPIC:-"streamflow.events"}

echo "Creating Kafka topic: $TOPIC on broker $BOOTSTRAP_SERVERS"
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --partitions 1 \
  --replication-factor 1 \
  --topic "$TOPIC"
