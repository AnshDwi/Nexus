#!/usr/bin/env bash
# Reset the LOCAL development stack to a clean slate.
#
# DELETES all local data: jobs and history (PostgreSQL), broker queues
# (RabbitMQ), rate-limit counters (Redis), metrics (Prometheus) and Grafana
# state. Only the local Docker Compose project "nexus" is affected; the
# production stack (compose.production.yml, project "nexus-prod") is not.
#
# Usage:  scripts/reset-local.sh          asks for confirmation
#         scripts/reset-local.sh --yes    no prompt (scripts/CI)
set -euo pipefail
cd "$(dirname "$0")/.."

volumes=$(docker volume ls -q --filter label=com.docker.compose.project=nexus)
echo "This stops every local NEXUS container and PERMANENTLY DELETES these volumes:"
if [ -n "$volumes" ]; then
  while read -r volume; do echo "  - $volume"; done <<< "$volumes"
else
  echo "  (none exist yet)"
fi
if [ "${1:-}" != "--yes" ]; then
  read -r -p "Type 'reset' to continue: " answer
  [ "$answer" = "reset" ] || { echo "Cancelled; nothing was changed."; exit 1; }
fi

# --profile "*" includes the optional services (monitoring, demo seed), so
# nothing is left running and holding the network.
docker compose --profile "*" down --volumes --remove-orphans
echo
echo "Reset complete. Start fresh with:  docker compose up --build -d"
