#!/usr/bin/env bash
# Applies db/shard-schema.sql identically to all 4 shard containers,
# idempotently. Postgres only auto-runs docker-entrypoint-initdb.d
# scripts against a *fresh* data volume, so this lets you re-apply schema
# changes without wiping any shard's data.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for i in 0 1 2 3; do
  echo "==> shard-$i"
  docker exec -i "ig-shard-$i" psql -U ig_admin -d "instagram_shard$i" < "$ROOT/db/shard-schema.sql"
done
echo "Schema applied to all 4 shards."
