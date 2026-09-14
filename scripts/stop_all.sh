#!/usr/bin/env bash
# Stops every service started by run_all.sh (leaves docker infra running --
# pass --infra to also stop that).
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -d .pids ]; then
  for pidfile in .pids/*.pid; do
    [ -f "$pidfile" ] || continue
    name="$(basename "$pidfile" .pid)"
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "Stopping $name (pid $pid)"
      kill "$pid" 2>/dev/null
    fi
    rm -f "$pidfile"
  done
fi

if [ "${1:-}" = "--infra" ]; then
  echo "Stopping docker infra"
  docker-compose down
fi

echo "Done."
