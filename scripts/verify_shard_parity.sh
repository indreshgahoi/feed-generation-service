#!/usr/bin/env bash
# Asserts the Go and Java consistent-hash ring implementations agree on
# shard placement for the same keys against the same config/shards.json.
# This is a correctness invariant, not a nice-to-have: if the two ever
# disagreed, a user created via one code path could end up with related
# data split across the wrong shards. See doc/sharding.md.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

CONFIG="$ROOT/config/shards.json"
N=${1:-10000}

echo "==> Building Go shardcli"
GO_BIN="$(mktemp -d)/shardcli-go"
(cd "$ROOT/pkg/sharding" && go build -o "$GO_BIN" ./cmd/shardcli)

echo "==> Building Java fanout-worker (for ShardCli)"
if [ ! -f "$ROOT/services/fanout-worker/target/fanout-worker-jar-with-dependencies.jar" ]; then
  (cd "$ROOT/services/fanout-worker" && mvn -q -B package -DskipTests)
fi
JAVA_JAR="$ROOT/services/fanout-worker/target/fanout-worker-jar-with-dependencies.jar"

echo "==> Generating $N sample placement keys"
KEYS_FILE="$(mktemp)"
for i in $(seq 1 "$N"); do echo "user-$i-$RANDOM"; done > "$KEYS_FILE"
mapfile -t KEYS < "$KEYS_FILE"

echo "==> Computing shard assignment with Go"
GO_OUT="$(mktemp)"
"$GO_BIN" "$CONFIG" "${KEYS[@]}" | sort > "$GO_OUT"

echo "==> Computing shard assignment with Java"
JAVA_OUT="$(mktemp)"
java -cp "$JAVA_JAR" com.feed.sharding.ShardCli "$CONFIG" "${KEYS[@]}" | sort > "$JAVA_OUT"

echo "==> Diffing $N placements"
if diff -q "$GO_OUT" "$JAVA_OUT" > /dev/null; then
  echo "PASS: Go and Java agree on all $N shard placements."
  rm -f "$KEYS_FILE" "$GO_OUT" "$JAVA_OUT" "$GO_BIN"
  exit 0
else
  echo "FAIL: Go and Java DISAGREE on shard placement. First differences:"
  diff "$GO_OUT" "$JAVA_OUT" | head -20
  rm -f "$KEYS_FILE" "$GO_OUT" "$JAVA_OUT" "$GO_BIN"
  exit 1
fi
