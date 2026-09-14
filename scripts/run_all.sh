#!/usr/bin/env bash
# Brings up infra (4 Postgres shards, Neo4j, Redis, Kafka, Qdrant, MinIO)
# + every service. Run scripts/build_all.sh first (or let this script
# build on first run). Logs go to logs/<service>.log, PIDs to
# .pids/<service>.pid so stop_all.sh can clean up.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
source scripts/env.sh
set -a; source .env; set +a

# Every service's SHARD_CONFIG_PATH default assumes it's run from its own
# cmd/server/ directory (../../config/shards.json). Binaries started below
# run with CWD = $ROOT instead, so this MUST be absolute here -- a
# relative default that "worked" during `cd cmd/server && go run .`
# development would silently resolve to the wrong path (or escape the
# repo entirely) once launched this way.
export SHARD_CONFIG_PATH="$ROOT/config/shards.json"

mkdir -p logs .pids

echo "==> Starting infra (4 Postgres shards, Neo4j, Redis, Kafka, Qdrant, MinIO)"
docker-compose up -d

echo "==> Waiting for all 4 Postgres shards to be healthy"
for i in 0 1 2 3; do
  until docker exec "ig-shard-$i" pg_isready -U ig_admin -d "instagram_shard$i" >/dev/null 2>&1; do
    sleep 1
  done
done

echo "==> Waiting for Neo4j to be ready"
until docker exec ig-graph-db cypher-shell -u neo4j -p feedpassword "RETURN 1" >/dev/null 2>&1; do
  sleep 1
done

echo "==> Applying schema to all 4 shards"
bash scripts/migrate.sh

echo "==> Creating Kafka topics"
sleep 2 # give the broker a moment after container start
bash scripts/create_topics.sh

start() {
  local name="$1"; shift
  echo "==> Starting $name"
  nohup "$@" > "logs/$name.log" 2>&1 &
  echo $! > ".pids/$name.pid"
}

# Build binaries if they don't exist yet.
[ -x services/post-ingestion-service/bin/post-ingestion-service ] || bash scripts/build_all.sh

start post-ingestion-service services/post-ingestion-service/bin/post-ingestion-service
start ranking-service services/ranking-service/target/release/ranking-service

mkdir -p services/feed-aggregation-service/data/cold-tier
COLD_TIER_DATA_DIR="$ROOT/services/feed-aggregation-service/data/cold-tier" \
  start feed-aggregation-service services/feed-aggregation-service/bin/feed-aggregation-service

(cd services/fanout-worker && nohup java -jar target/fanout-worker-jar-with-dependencies.jar > "$ROOT/logs/fanout-worker.log" 2>&1 & echo $! > "$ROOT/.pids/fanout-worker.pid")
(cd services/vector-pipeline && nohup python3 main.py > "$ROOT/logs/vector-pipeline.log" 2>&1 & echo $! > "$ROOT/.pids/vector-pipeline.pid")
(cd services/notification-service && nohup npx tsx src/index.ts > "$ROOT/logs/notification-service.log" 2>&1 & echo $! > "$ROOT/.pids/notification-service.pid")

WEB_UI_PORT="${WEB_UI_PORT:-5173}"
(cd web-ui && nohup python3 serve.py "$WEB_UI_PORT" > "$ROOT/logs/web-ui.log" 2>&1 & echo $! > "$ROOT/.pids/web-ui.pid")

echo ""
echo "All services starting. Tail logs with: tail -f logs/*.log"
echo "  web UI                   http://localhost:${WEB_UI_PORT}"
echo "  post-ingestion-service   http://localhost:${INGESTION_PORT}"
echo "  feed-aggregation-service http://localhost:${FEED_PORT}"
echo "  ranking-service          http://localhost:${RANKING_PORT}"
echo "  Neo4j browser            http://localhost:7474 (neo4j / feedpassword)"
echo ""
echo "Seed demo data with: python3 scripts/seed.py"
echo "Verify Go/Java shard-routing parity with: bash scripts/verify_shard_parity.sh"
echo "Stop everything with: bash scripts/stop_all.sh"
