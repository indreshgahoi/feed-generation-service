#!/usr/bin/env bash
# Brings up the ENTIRE system (4 Postgres shards, Neo4j, Redis, Kafka,
# Qdrant, MinIO, all 7 application services, and the Envoy gateway) as
# Docker Compose containers. Nothing runs as a bare host process anymore
# -- see doc/DESIGN.md and README.md for why. Logs: `docker compose logs
# -f <service>`. Stop everything with scripts/stop_all.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "==> Generating Envoy's dev TLS cert if missing (needed for the HTTP/3 listener)"
bash scripts/gen_dev_certs.sh

echo "==> Building and starting every container"
docker-compose up -d --build

echo "==> Waiting for the gateway to come up"
until curl -sf http://localhost:9901/ready >/dev/null 2>&1; do
  sleep 1
done

echo ""
echo "All services are up behind the Envoy gateway."
echo "  web UI                   http://localhost:8080"
echo "  post-ingestion API       http://localhost:8080/api/ingest"
echo "  feed-aggregation API     http://localhost:8080/api/feed  (HTTP/1.1 + h2)"
echo "                           https://localhost:8443/api/feed (HTTP/3 / QUIC)"
echo "  Envoy admin/stats        http://localhost:9901"
echo "  Neo4j browser            http://localhost:7474 (neo4j / feedpassword)"
echo ""
echo "Tail logs with: docker-compose logs -f <service>"
echo "Seed demo data with: python3 scripts/seed.py"
echo "Verify Go/Java shard-routing parity with: bash scripts/verify_shard_parity.sh"
echo "Stop everything with: bash scripts/stop_all.sh"
