#!/usr/bin/env bash
# ==============================================================================
# Kafka Topic Provisioning Script
# ==============================================================================
# Automates the creation and configuration of Apache Kafka topics required for
# the Event-Sourced Financial Ledger (ESFL) platform.
#
# Prerequisites:
#   kafka-topics.sh must be on PATH, or override via the KAFKA_TOPICS_CMD
#   environment variable, e.g.:
#     KAFKA_TOPICS_CMD=/opt/kafka/bin/kafka-topics.sh ./provision-kafka.sh
#
# Usage:
#   ./provision-kafka.sh [bootstrap-server]   (default: localhost:9092)
#
# Configurations enforced:
# - account-events      : 12 partitions, replication-factor=3, retention=90d,  min.insync.replicas=2
# - transfer-saga-events: 12 partitions, replication-factor=3, retention=30d,  min.insync.replicas=2
# - account-events-dlq  :  1 partition,  replication-factor=3, retention=365d, min.insync.replicas=2
# ==============================================================================

set -euo pipefail

BOOTSTRAP_SERVER=${1:-"localhost:9092"}
KAFKA_TOPICS_CMD=${KAFKA_TOPICS_CMD:-"kafka-topics.sh"}

echo "----------------------------------------------------------------"
echo "Initializing Kafka Topic Provisioning"
echo "Bootstrap Server: ${BOOTSTRAP_SERVER}"
echo "----------------------------------------------------------------"

# Function to check if Kafka is ready
wait_for_kafka() {
  echo "Waiting for Kafka broker at ${BOOTSTRAP_SERVER} to be ready..."
  local retries=30
  local wait_sec=2
  for i in $(seq 1 $retries); do
    if ${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP_SERVER}" --list > /dev/null 2>&1; then
      echo "Kafka is ready!"
      return 0
    fi
    echo "Broker not ready yet (attempt $i/$retries). Retrying in ${wait_sec}s..."
    sleep $wait_sec
  done
  echo "Error: Kafka broker at ${BOOTSTRAP_SERVER} failed to become ready in time." >&2
  return 1
}

# Ensure Kafka is reachable
wait_for_kafka

# Topic configurations
declare -A TOPICS
# Format: [topic_name]="partitions replication_factor min_insync_replicas retention_ms"
TOPICS["account-events"]="12 3 2 7776000000"
TOPICS["transfer-saga-events"]="12 3 2 2592000000"
TOPICS["account-events-dlq"]="1 3 2 31536000000"

echo "Provisioning topics..."
for topic in "${!TOPICS[@]}"; do
  # Parse config parameters
  read -r partitions rf min_isr retention <<< "${TOPICS[$topic]}"
  
  echo ""
  echo "Configuring topic: '${topic}'"
  echo "  - Partitions: ${partitions}"
  echo "  - Replication Factor: ${rf}"
  echo "  - Min In-Sync Replicas: ${min_isr}"
  echo "  - Retention (ms): ${retention}"
  
  # Create the topic with the required settings if it does not exist.
  # The subshell captures failure so we can surface which topic caused the error.
  if ! ${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --config min.insync.replicas="${min_isr}" \
    --config retention.ms="${retention}"; then
    echo "Error: failed to provision topic '${topic}'. Aborting." >&2
    exit 1
  fi
done

echo ""
echo "================================================================"
echo "Verifying Topic Configurations"
echo "================================================================"
for topic in "${!TOPICS[@]}"; do
  echo "Topic: ${topic}"
  ${KAFKA_TOPICS_CMD} --bootstrap-server "${BOOTSTRAP_SERVER}" --describe --topic "${topic}"
  echo "----------------------------------------------------------------"
done

echo "Verification complete. All topics provisioned successfully."
