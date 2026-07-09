#!/bin/bash
set -e

CONFIG_FILE="/opt/streamflow/config/pipeline.yml"

# Verify broker is responding
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 || exit 1

# Parse topic name from the yaml config
if [ -f "$CONFIG_FILE" ]; then
    TOPIC=$(grep -A 2 'kafka:' "$CONFIG_FILE" | grep 'topic:' | awk '{print $2}' | tr -d '"' | tr -d "'")
fi
TOPIC=${TOPIC:-"streamflow.events"}

# Verify the topic exists
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q "^${TOPIC}$"
