#!/usr/bin/env bash
# Pre-creates Kafka topics. Auto-create-on-first-produce is racy (the first
# publish attempt fails with "Unknown Topic Or Partition" while the topic is
# still being created), so we create them explicitly up front instead.
set -euo pipefail

docker exec ig-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic post-created --partitions 3 --replication-factor 1

docker exec ig-kafka kafka-topics --bootstrap-server localhost:9092 --list
