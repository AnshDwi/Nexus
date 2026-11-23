#!/usr/bin/env bash
# Run the backend integration tests against the local Docker Compose stack
# WITHOUT touching your development data.
#
# The integration tests truncate tables and purge queues. Pointed at the dev
# database and broker they would wipe your jobs and steal messages from the
# running workers. This script gives them their own space on the same
# containers:
#   PostgreSQL  database  nexus_test   (created and migrated here)
#   RabbitMQ    vhost     nexus-test   (created here; dev workers use "/")
#   Redis       db 15     (the one Redis test already uses db 15; dev uses db 0)
#
# Usage:  scripts/test-integration.sh [extra pytest args]
# Needs:  the stack running (docker compose up -d) and uv on the host.
set -euo pipefail
cd "$(dirname "$0")/.."

running() { docker compose ps --status running "$1" 2>/dev/null | grep -q "$1"; }
for svc in postgres rabbitmq redis; do
  running "$svc" || { echo "error: '$svc' is not running. Start the stack first: docker compose up -d" >&2; exit 1; }
done

user="${POSTGRES_USER:-nexus}"
psql() { docker compose exec -T -e PGPASSWORD="${POSTGRES_PASSWORD:-nexus}" postgres psql -h 127.0.0.1 -U "$user" "$@"; }
echo "== preparing isolated test database (nexus_test) and broker vhost (nexus-test)"
psql -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'nexus_test'" | grep -q 1 \
  || psql -d postgres -qc "CREATE DATABASE nexus_test"
docker compose exec -T rabbitmq rabbitmqctl -q add_vhost nexus-test 2>/dev/null || true
docker compose exec -T rabbitmq rabbitmqctl -q set_permissions -p nexus-test "${RABBITMQ_USER:-nexus}" ".*" ".*" ".*"

export ENVIRONMENT=test
export POSTGRES_HOST=localhost POSTGRES_PORT="${POSTGRES_HOST_PORT:-5432}" POSTGRES_DB=nexus_test
export RABBITMQ_URL="amqp://${RABBITMQ_USER:-nexus}:${RABBITMQ_PASSWORD:-nexus}@localhost:${RABBITMQ_HOST_PORT:-5672}/nexus-test"
export REDIS_URL="redis://localhost:${REDIS_HOST_PORT:-6379}/15"

echo "== migrating nexus_test"
uv run alembic upgrade head
echo "== running integration tests"
uv run pytest -m integration "$@"
