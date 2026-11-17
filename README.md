# NEXUS

A durable, multi-tenant task execution platform. It covers:
- **Submission:** clients submit tasks over an authenticated HTTP API, and an admission controller validates them.
- **Persistence:** tasks are stored in PostgreSQL with a full transition history.
- **Dispatch:** tasks go out through a transactional outbox to Vercel Queues.
- **Execution:** tasks run with timeouts, retries with exponential backoff, dependencies (DAGs) and cancellation.
- **Recovery:** a reconciler repairs anything a crash leaves behind.

The deployment target is **Vercel**. See [docs/vercel-deployment.md](docs/vercel-deployment.md) and [ADR 0004](docs/adr/0004-vercel-first-execution.md).

## Architecture

The code follows Clean Architecture. Dependencies point inward.

```
src/nexus/
  domain/          pure rules: task state machine, readiness, admission, retry/lifecycle
  application/     ports (Protocols) and use cases: submit, claim/complete/fail, cancel, reconcile
  infrastructure/  PostgreSQL, outbox/inbox, leader lease, Vercel Queues / RabbitMQ, observability
  api/             FastAPI routers, auth
  entrypoints/     queue subscriber + worker runtime (non-API processes)
  daemon.py        scheduler / poll-worker daemons
vercel_app/        Vercel entrypoint shims (src/ layout → importable)
```

The core patterns are documented as ADRs:
- [Inbox](docs/adr/0001-exactly-once-inbox-pattern.md)
- [Outbox](docs/adr/0002-transactional-outbox-pattern.md)
- [Leader lease with fencing tokens](docs/adr/0003-scheduler-leader-election.md)
- [Vercel-first execution](docs/adr/0004-vercel-first-execution.md)

## Quick start (local)

```bash
uv sync                                    # Python 3.12 (.python-version)
cp .env.example .env
docker compose up --build                  # postgres, queue devserver, migrate, api, scheduler, worker
curl -s localhost:8000/api/v1/ready

curl -s -X POST localhost:8000/api/v1/tasks \
  -H "Authorization: Bearer dev-key" -H "Content-Type: application/json" \
  -d '{"task_type":"echo","payload":{"hello":"world"},
       "retry_policy":{"max_attempts":3,"initial_backoff_seconds":1,"max_backoff_seconds":30}}'
curl -s localhost:8000/api/v1/tasks/<task_id> -H "Authorization: Bearer dev-key"
```

To run without Docker, start each piece yourself:

```bash
uv run python -m vercel.queue.devserver --port 9100
uv run alembic upgrade head
uv run uvicorn vercel_app.api:app --reload
uv run python -m nexus.daemon scheduler
DAEMON_HEALTH_PORT=8082 uv run python -m nexus.daemon worker
```

## API (all under `/api/v1`, Bearer API key)

| Method | Path | |
|---|---|---|
| POST | `/tasks` | Submit. `Idempotency-Key` header supported. 201 created / 200 duplicate / 422 rejected (with admission detail) |
| GET | `/tasks` | List own tenant's tasks (`state`, `limit`, `created_before`) |
| GET | `/tasks/{id}` | Status, attempt, result, last error |
| GET | `/tasks/{id}/transitions` | Full audit trail |
| POST | `/tasks/{id}/cancel` | Cancel; dependents are cancelled too |
| GET | `/health`, `/ready` | Liveness / readiness (503 when degraded) |
| GET | `/internal/maintenance` | Cron sweep (`CRON_SECRET`) |
| GET | `/internal/metrics` | Prometheus (`METRICS_TOKEN`) |

OpenAPI: `/api/docs`.

## Development

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src
uv run pytest                                   # unit
uv run alembic upgrade head && uv run pytest -m integration   # needs Postgres (+ RabbitMQ for 1 legacy test)
```

CI (`.github/workflows/ci.yml`) runs these steps:
1. Lint, types and unit tests.
2. Integration tests, plus `alembic check` and a downgrade/upgrade round trip.
3. An offline `vercel build` with assertions on the generated functions, queue trigger and cron.
4. On `main` only, once the deploy secrets are configured: migrate, deploy, then verify end to end.
