#!/usr/bin/env bash
# Asserts the generated protobuf/FlatBuffers code committed in each
# service's tree actually matches what schemas/proto and schemas/fbs would
# regenerate right now. This is the same idiom as verify_shard_parity.sh:
# a claim about generated code being "in sync with its schema" is only as
# good as the mechanism that checked it, not an assumption that nobody
# forgot to re-run scripts/gen_schemas.sh after editing a .proto/.fbs file.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SNAPSHOT="$(mktemp -d)"
trap 'rm -rf "$SNAPSHOT"' EXIT

# Every path scripts/gen_schemas.sh writes into.
GENERATED_PATHS=(
  services/feed-aggregation-service/internal/genproto
  services/feed-aggregation-service/internal/genfbs
  services/post-ingestion-service/internal/genfbs
  services/fanout-worker/src/main/java/feed
  services/vector-pipeline/genfbs
  services/notification-service/src/genfbs
  services/ranking-service/src/genfbs
)

echo "==> Snapshotting current generated code"
for p in "${GENERATED_PATHS[@]}"; do
  if [ -d "$ROOT/$p" ]; then
    mkdir -p "$SNAPSHOT/$p"
    cp -r "$ROOT/$p/." "$SNAPSHOT/$p/"
  fi
done

echo "==> Regenerating from schemas/proto and schemas/fbs"
bash "$ROOT/scripts/gen_schemas.sh" > /dev/null

echo "==> Diffing regenerated code against the snapshot"
FAILED=0
for p in "${GENERATED_PATHS[@]}"; do
  if ! diff -rq -x __pycache__ "$SNAPSHOT/$p" "$ROOT/$p" > /tmp/schema_gen_diff.$$ 2>&1; then
    echo "FAIL: $p is out of date with its schema:"
    cat /tmp/schema_gen_diff.$$
    FAILED=1
  fi
  rm -f /tmp/schema_gen_diff.$$
done

if [ "$FAILED" -ne 0 ]; then
  echo
  echo "One or more generated-code directories don't match schemas/. Run"
  echo "scripts/gen_schemas.sh and commit the result."
  exit 1
fi

echo "PASS: all generated code matches schemas/proto and schemas/fbs."
