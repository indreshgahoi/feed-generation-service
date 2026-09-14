#!/usr/bin/env bash
# Stops the whole stack -- everything (infra AND application services) is
# a Docker Compose container now, so there's no separate host-process
# bookkeeping (.pids/, nohup) left to clean up.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker-compose down
echo "Done. (Data volumes are preserved -- add -v to the docker-compose down call in this script to also wipe them.)"
