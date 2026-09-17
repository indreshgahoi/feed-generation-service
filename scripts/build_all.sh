#!/usr/bin/env bash
# Compiles every service once so run_all.sh can just exec fast binaries
# instead of re-compiling (go run/cargo run) on every start.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

echo "==> pkg/sharding (Go) -- shared consistent-hash ring + self-routing IDs"
(cd "$ROOT/pkg/sharding" && go build ./... && go test ./...)

echo "==> post-ingestion-service (Go)"
(cd "$ROOT/services/post-ingestion-service" && go build -o bin/post-ingestion-service ./cmd/server)

echo "==> feed-aggregation-service (Go)"
(cd "$ROOT/services/feed-aggregation-service" && go build -o bin/feed-aggregation-service ./cmd/server)

echo "==> ranking-service (Rust)"
(cd "$ROOT/services/ranking-service" && cargo build --release --quiet)

echo "==> fanout-worker (Java, includes com.feed.sharding parity implementation)"
(cd "$ROOT/services/fanout-worker" && mvn -q -B package)

echo "==> notification-service (Node/TypeScript) deps"
(cd "$ROOT/services/notification-service" && npm install --no-fund --no-audit --silent)

echo "==> vector-pipeline (Python) deps"
pip3 install --user --quiet -r "$ROOT/services/vector-pipeline/requirements.txt"
pip3 install --user --quiet -r "$ROOT/scripts/requirements.txt"

echo "==> verifying Go/Java shard-routing parity"
bash "$ROOT/scripts/verify_shard_parity.sh" 2000

echo "==> verifying generated protobuf/FlatBuffers code matches schemas/"
bash "$ROOT/scripts/verify_schema_gen.sh"

echo "All services built."
