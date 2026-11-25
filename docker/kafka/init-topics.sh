#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be set}"
topics_cli=/opt/kafka/bin/kafka-topics.sh
configs_cli=/opt/kafka/bin/kafka-configs.sh

echo 'Creating homework topics (existing topics are kept)...'
"$topics_cli" --bootstrap-server "$bootstrap_servers" \
  --create --if-not-exists --topic events \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

"$topics_cli" --bootstrap-server "$bootstrap_servers" \
  --create --if-not-exists --topic events.dlt \
  --partitions 3 --replication-factor 3

# Reapply the required topic setting when initialization is run again.
"$configs_cli" --bootstrap-server "$bootstrap_servers" \
  --alter --entity-type topics --entity-name events \
  --add-config min.insync.replicas=2

for topic in events events.dlt; do
  "$topics_cli" --bootstrap-server "$bootstrap_servers" \
    --describe --topic "$topic"
done

echo 'Topic initialization completed.'
