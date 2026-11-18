# NEXUS — Internal Developer Platform

NEXUS runs your platform's background jobs reliably and shows you exactly what happened to each one.

Submit a job from the dashboard or the API. NEXUS then:
- stores it in PostgreSQL;
- dispatches it through RabbitMQ, and only marks it queued once the broker confirms the message;
- runs it on a registered, heartbeating worker with a timeout;
- retries failures with exponential backoff;
- dead-letters what cannot succeed;
- records every attempt.

```
Dashboard (React) ──► API (FastAPI) ──► PostgreSQL ◄──────────────────────┐
                         │  jobs + outbox (one transaction)                │
                         ▼                                                 │
                   outbox relay ──publisher confirm──► RabbitMQ ──► Worker(s)
                         ▲                 nexus.jobs (priority)   register/heartbeat
                         │                 nexus.dead-letter       execute, report
                  Scheduler (leader-elected, fencing token):
                  relay retries · dead-worker detection · lease expiry
```

---

## Contents

1. [Quick start](#quick-start)
2. [Job lifecycle](#job-lifecycle)
3. [Architecture](#architecture)
4. [API reference](#api-reference)
5. [Configuration](#configuration)
6. [Testing](#testing)
7. [Deploying to Vercel](#deploying-to-vercel)
8. [Known limitations](#known-limitations)

---

## Quick start

### With Docker Compose (recommended)

Requires only Docker with Compose v2. Nothing else needs to be installed or running.

```bash
git clone <repo-url> nexus && cd nexus
cp .env.example .env            # optional: compose has working local defaults
docker compose up --build -d --wait
docker compose ps               # every service healthy; migrate "exited (0)"
```

`--wait` returns once every health check passes. That takes about 20 s with cached images, and longer on the first build.

| Service | What it is | Host address |
|---|---|---|
| `postgres` | PostgreSQL 16 (volume `postgres_data`) | 127.0.0.1:5432 |
| `rabbitmq` | RabbitMQ 3.13 + management UI (volume `rabbitmq_data`) | 127.0.0.1:5672, http://localhost:15672 (`nexus`/`nexus`) |
| `redis` | Shared rate-limit counters (volume `redis_data`) | 127.0.0.1:6379 |
| `migrate` | One-shot `alembic upgrade head`; everything below waits for it | — |
| `api` | FastAPI (`/api/v1/ready` health check) | http://localhost:8000/api/docs |
| `scheduler` | Outbox relay, retry dispatch, dead-worker and lease recovery | — |
| `worker` | RabbitMQ worker, capacity `WORKER_CAPACITY` (4) | — |
| `worker-small` | Second worker, capacity `WORKER_SMALL_CAPACITY` (2) | — |
| `dashboard` | Production build served by `vite preview`, proxying `/api` to `api` | http://localhost:5173 |

Startup order is enforced with health checks:
1. `postgres`, `rabbitmq` and `redis` become healthy.
2. `migrate` runs and exits 0.
3. `api`, `scheduler` and the workers start.
4. `dashboard` starts once `api` is healthy.

All ports bind to 127.0.0.1. Change any of them with `*_HOST_PORT` in `.env`.

**Walkthrough** (dashboard at http://localhost:5173, API key `dev-key`):

1. **Submit a job.** *Jobs → Create job*, type `echo`, payload `{"msg":"hi"}`.
2. **View its execution.** Open the job. Its timeline shows PENDING → QUEUED → RUNNING → COMPLETED, with the attempt, worker and duration. *Executions* lists every attempt.
3. **Trigger a failure and a retry.** Create a `test.flaky` job with payload `{"succeed_on_attempt": 3}` and *Max attempts* 2. It fails, waits out the backoff (RETRYING), fails again and is dead-lettered. Click **Retry** and it completes on attempt 3.
4. **View dead-lettered jobs.** Use the *Dead-lettered* chip on *Jobs*, or *Queue → View dead-lettered jobs*. The broker side is the `nexus.dead-letter` queue in the RabbitMQ UI.

The same flow with curl:

```bash
KEY="Authorization: Bearer dev-key"
JOB=$(curl -s -X POST localhost:8000/api/v1/jobs -H "$KEY" -H "Content-Type: application/json" \
  -d '{"name":"flaky","job_type":"test.flaky","payload":{"succeed_on_attempt":3},"max_attempts":2,"initial_backoff_seconds":1}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["job_id"])')
sleep 8; curl -s localhost:8000/api/v1/jobs/$JOB -H "$KEY"            # state failed, dead_lettered_at set
curl -s "localhost:8000/api/v1/jobs?dead_lettered=true" -H "$KEY"       # dead-letter list
curl -s -X POST localhost:8000/api/v1/jobs/$JOB/retry -H "$KEY"         # manual retry → completes on attempt 3
```

**Demo scenario (optional, opt-in).** This loads a repeatable scenario through the public API:
- one successful job;
- one retryable failure that recovers on attempt 2;
- one permanent failure that is not retried;
- one job that exhausts its retries and is dead-lettered;
- eight load jobs that occupy both workers.

```bash
docker compose --profile demo run --rm seed
```

Every number on the dashboard then comes from these real jobs, which really executed. Nothing is written to the database directly. The seed:
- refuses to run unless `ENVIRONMENT` is `local`, `test` or `development`;
- requires the `test.*` job types;
- names its jobs `demo-<run>/…`;
- checks every expected outcome and exits non-zero if any differs.

You can run it repeatedly; each run uses fresh idempotency keys.

**Stop and reset:**

```bash
docker compose down        # stop; data in the volumes is kept
docker compose down -v     # stop and delete all data
```

`docker compose stop` sends SIGTERM. Workers drain: they stop consuming, let in-flight jobs finish within `WORKER_SHUTDOWN_GRACE_SECONDS`, hand unfinished attempts back as RETRYING and deregister. Each worker's `stop_grace_period` is longer than that grace.

**Base images.** The defaults are `python:3.12-slim`, `node:22-alpine`, `postgres:16-alpine`, `rabbitmq:3.13-management-alpine` and `redis:7-alpine`. To use a mirror, set `NEXUS_PYTHON_IMAGE`, `NEXUS_NODE_IMAGE`, `NEXUS_POSTGRES_IMAGE`, `NEXUS_RABBITMQ_IMAGE` or `NEXUS_REDIS_IMAGE`.

### Without Docker

Requires Python 3.12 + [uv](https://docs.astral.sh/uv/), Node 22, and PostgreSQL 16, RabbitMQ 3.12+ and Redis running locally.

```bash
uv sync && cp .env.example .env
uv run alembic upgrade head
uv run uvicorn vercel_app.api:app --reload                    # API on :8000
uv run python -m nexus.daemon scheduler                       # relay + recovery
DAEMON_HEALTH_PORT=8082 uv run python -m nexus.daemon worker  # RabbitMQ worker
cd frontend && npm ci && npm run dev                          # dashboard on :5173
uv run python -m nexus.demo seed --wait                       # optional demo scenario
```

---

## Job lifecycle

```
PENDING ──► QUEUED ──► RUNNING ──► COMPLETED
   │          ▲           │
   │          │           ▼
   │       RETRYING ◄── FAILED   (automatic while attempts remain;
   │                      │       manual "Retry" after a final failure)
   └──────────┴───────────┴────► CANCELLED
```

| Rule | Mechanism |
|---|---|
| The job row and its dispatch message are created atomically | Transactional outbox (same DB transaction as the job) |
| QUEUED only after the broker accepted the message | Relay publishes with **publisher confirms** and `mandatory` routing; QUEUED is written in the same transaction that observed the confirm |
| Retries are delayed | FAILED → RETRYING writes a dispatch held back until `next_attempt_at` (exponential backoff, capped) |
| Each delivery runs at most once | Inbox record per message (`job-executor:{message_id}`) + attempt check |
| Stale workers cannot change state | Results accepted only from the worker holding the live attempt (`attempt` + `worker_id` fence) |
| Stale leaders cannot change state | Scheduler writes run under a `FOR SHARE` check of its lease **fencing token** |
| Dead workers' jobs are retried | Heartbeat TTL → scheduler fails and retries their RUNNING jobs |
| Hung jobs are retried | Execution lease = timeout + grace; expiry → FAILED → RETRYING |
| Final failures are dead-lettered | Job FAILED with `dead_lettered_at`; message rejected to `nexus.dead-letter` |
| Graceful shutdown | SIGTERM: draining, stop consuming, finish in-flight work (grace period), hand unfinished attempts back, deregister |

---

## Architecture

The code follows Clean Architecture: dependencies point inward, and the domain is pure and fully unit-tested.

```
src/nexus/
  domain/          job.py (Job aggregate + state machine), task_state, admission, leader_election…
  application/     ports (Protocols) + use cases: jobs.py (create, dispatch hook, claim,
                   report, cancel, retry, recover), relay_outbox_once, admission_controller…
  infrastructure/  db (models, repositories), messaging (RabbitMQ topology/publisher/worker,
                   Vercel Queues adapter), coordination (leader lease, worker registry),
                   execution (job executor, handlers, maintenance), observability
  api/             FastAPI routers (jobs, platform, tasks, health, internal), auth, rate limiting
  daemon.py        `scheduler` and `worker` processes
frontend/          React + TypeScript + Vite + Tailwind dashboard
vercel_app/        Vercel entrypoint shims (src/ layout)
migrations/        Alembic
```

Design records:
- [ADR 0001: Inbox](docs/adr/0001-exactly-once-inbox-pattern.md)
- [ADR 0002: Outbox](docs/adr/0002-transactional-outbox-pattern.md)
- [ADR 0003: Leader lease and fencing](docs/adr/0003-scheduler-leader-election.md)
- [ADR 0004: Vercel-first execution](docs/adr/0004-vercel-first-execution.md)
- [ADR 0005: Job pipeline over RabbitMQ](docs/adr/0005-job-pipeline-rabbitmq.md)

### How the scheduler works

`python -m nexus.daemon scheduler` runs a loop. Any number of replicas may run; one at a time holds the leader lease, a PostgreSQL row with a monotonically increasing **fencing token**. Every write the leader makes re-checks its token under `SELECT … FOR SHARE`, so a paused, deposed leader cannot commit. Each pass the leader:

1. **Relays the outbox.** It publishes due dispatch messages (`available_at <= now()`) to the `nexus.jobs` exchange with publisher confirms. A message only marks its job QUEUED after the broker confirms it. Unroutable or nacked messages stay in the outbox and are retried.
2. **Detects dead workers.** It marks workers whose heartbeat is older than `WORKER_HEARTBEAT_TTL_SECONDS` as unhealthy. Their RUNNING jobs are failed and retried.
3. **Expires leases.** It fails a RUNNING attempt whose execution lease (timeout + grace) has expired, which retries it or dead-letters it.

On Vercel, the same pass also runs from the `/internal/maintenance` cron as a backstop.

### How workers execute jobs

`python -m nexus.daemon worker`:

1. **Registers.** Records id, hostname and capacity, and heartbeats on database time.
2. **Consumes.** Takes from `nexus.jobs` with `prefetch = capacity`.
3. **Claims each delivery** in one transaction:
   - records an inbox entry (`job-executor:{message_id}`), so a redelivery is a no-op;
   - checks that the message's attempt is the job's current one;
   - moves the job QUEUED → RUNNING, with an execution row and lease.
4. **Runs the handler** with the job's timeout.
5. **Reports** COMPLETED or FAILED, fenced on `attempt` + `worker_id`, so a result from a worker that lost the job is discarded.

On failure:
- **Retryable:** the job moves FAILED → RETRYING and a new dispatch is written to the outbox, held back until `next_attempt_at`. The backoff is `initial_backoff_seconds · 2^(attempt-1)`, capped at `max_backoff_seconds`.
- **Permanent, or the last attempt:** the job stays FAILED with `dead_lettered_at` set, and the message is rejected into `nexus.dead-letter`.

`POST /jobs/{id}/retry` gives a FAILED job a fresh attempt budget.

**Job types.** Workers only run registered handlers, never code supplied in a request:

| Type | What it does |
|---|---|
| `echo` | Returns its payload |
| `http.webhook` | POSTs to hosts in `WEBHOOK_ALLOWED_HOSTS` |
| `test.sleep`, `test.flaky`, `test.fail` | Only when `TASK_ENABLE_TEST_HANDLERS=true` |

Add a type in `infrastructure/execution/handlers.py`.

**Tasks API.** The earlier `/api/v1/tasks` DAG pipeline (dependencies, `not_before`, Vercel Queues push consumer) is unchanged and is also executed by the RabbitMQ worker (`nexus.tasks` queue).

---

## API reference

Everything is under `/api/v1`. Authenticate with `Authorization: Bearer <api key>`.

- **Keys are tenant-scoped.** Another tenant's job returns 404.
- **Errors** look like `{"detail": {"code", "message", "details?"}}`. Internals are never exposed, and 500s carry a `request_id`.
- **Every response** carries `X-Request-ID` and `X-Correlation-ID`. Send `X-Correlation-ID` to trace a job across API, relay and worker logs.
- **Interactive docs:** `/api/docs`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Create. Body: `name`, `job_type`, `payload`, `priority` (`low\|normal\|high\|critical`), `max_attempts` (1–20), `timeout_seconds`, `initial_backoff_seconds`, `max_backoff_seconds`. `Idempotency-Key` header: replays return the original job (200). 201 created · 422 invalid/rejected · 413 payload too large · 429 rate limited |
| `GET` | `/jobs` | List with pagination: `page`, `page_size` (≤100), `status` (repeatable), `priority` (repeatable), `q` (name/type/ID search), `dead_lettered=true` (only dead-lettered jobs) |
| `GET` | `/jobs/{id}` | Job, state transitions and execution attempts |
| `POST` | `/jobs/{id}/cancel` | Cancel PENDING/QUEUED/RETRYING/RUNNING (409 otherwise). A running attempt's result is discarded |
| `POST` | `/jobs/{id}/retry` | Retry a FAILED job with a fresh attempt budget (409 otherwise) |
| `GET` | `/workers` | Registered workers: status (healthy/unhealthy/draining), heartbeat age, running jobs vs capacity |
| `GET` | `/queue` | Pending/queued/retrying jobs by priority with dispatch status; `dead_lettered_jobs` (database) and broker ready/dead-letter message counts |
| `GET` | `/executions` | Execution history: `status`, `job_id`, `page`, `page_size` |
| `GET` | `/metrics/summary` | Overview: totals by state, queue depth, worker health, recent activity, hourly trend |
| `GET` | `/system` | Dependency checks, job types, non-secret configuration |
| `GET` | `/health` · `/ready` | Liveness · readiness (503 when a configured dependency fails) |
| `GET` | `/internal/maintenance` | Scheduler pass (`Bearer $CRON_SECRET`) |
| `GET` | `/internal/metrics` | Prometheus (`Bearer $METRICS_TOKEN`) |
| | `/tasks…` | DAG task API (see ADR 0004) |

---

## Configuration

All configuration comes from environment variables (see `.env.example`). Outside `local`/`test`, the API **refuses to start** if any of these hold:
- database or RabbitMQ credentials are the built-in defaults;
- `API_KEYS` is empty;
- `DEBUG` is on;
- CORS origins are `*` or not https.

| Variable | Purpose |
|---|---|
| `DATABASE_URL` / `DATABASE_URL_UNPOOLED` | Postgres (provider URLs accepted; `sslmode` handled; PgBouncer auto-detected). Discrete `POSTGRES_*` also supported |
| `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_MODE` | Pooling: small per-instance pool (`queue`, Fluid-compute safe) or `null` behind an external pooler |
| `RABBITMQ_URL` | Broker (`amqps://…` for managed brokers). `EVENT_BUS=rabbitmq` (default) \| `vercel_queues` \| `none` |
| `REDIS_URL` | Shared rate-limit counters (optional; falls back to per-process) |
| `API_KEYS` | `tenant:sha256(key),…`. Generate with `uv run python -m nexus.cli new-api-key <tenant>` |
| `CORS_ALLOWED_ORIGINS` | Dashboard origin(s) when served from another domain |
| `RATE_LIMIT_READS_PER_MINUTE`, `RATE_LIMIT_WRITES_PER_MINUTE` | Per tenant |
| `WORKER_CAPACITY`, `WORKER_HEARTBEAT_*`, `WORKER_SHUTDOWN_GRACE_SECONDS` | Worker behaviour |
| `JOB_DEFAULT_TIMEOUT_SECONDS`, `JOB_MAX_TIMEOUT_SECONDS`, `JOB_DEFAULT_MAX_ATTEMPTS` | Job limits |
| `CRON_SECRET`, `METRICS_TOKEN` | Protect internal endpoints |
| `TASK_ENABLE_TEST_HANDLERS` | Registers `test.sleep`/`test.flaky`/`test.fail` (local and CI only) |
| `WORKER_SMALL_CAPACITY` | Compose only: capacity of the second worker |
| `*_HOST_PORT` | Compose only: host ports (`API_`, `DASHBOARD_`, `POSTGRES_`, `RABBITMQ_`, `RABBITMQ_MANAGEMENT_`, `REDIS_`) |
| `NEXUS_*_IMAGE` | Compose only: base-image overrides (mirrors) |
| `DEMO_API_KEY` | Compose only: key used by the `seed` service (default `dev-key`) |

Logs are JSON lines carrying `request_id`, `correlation_id`, `job_id` and `attempt`.

---

## Testing

```bash
# Backend
uv run ruff check . && uv run ruff format --check . && uv run mypy src
uv run pytest                                        # unit tests
uv run alembic upgrade head && uv run pytest -m integration   # needs Postgres, RabbitMQ, Redis

# Frontend
cd frontend && npm ci && npm run typecheck && npm run lint && npm test && npm run build

# Migrations: upgrade, model/migration drift check, full downgrade
uv run alembic upgrade head && uv run alembic check && uv run alembic downgrade base

# Whole stack, end to end against the running containers
docker compose config --quiet
docker compose up --build -d --wait
uv run pytest -m e2e                                  # 8 lifecycle tests over HTTP
docker compose --profile demo run --rm seed           # scenario + outcome verification
docker compose down -v
```

The `e2e` suite (`tests/e2e/test_compose_stack.py`) uses only the public API and the dashboard. Point it elsewhere with `NEXUS_E2E_API_URL`, `NEXUS_E2E_DASHBOARD_URL` and `NEXUS_E2E_API_KEY`. It checks:
- dependency readiness;
- both workers and their capacities;
- the dashboard's SPA fallback and API proxy;
- full lifecycle transitions;
- flaky-job retry;
- exhaustion → dead-letter (database and broker) → manual retry;
- a permanent failure making only one attempt;
- summary counts that match the listing endpoints.

What the suites cover:
- **Integration tests** run against real PostgreSQL, RabbitMQ and Redis:
  - job lifecycle and retries;
  - dead-lettering and manual retry;
  - timeouts and idempotency;
  - unroutable publishes never being marked QUEUED;
  - dead-worker recovery and fencing of stale workers and leaders;
  - graceful shutdown and the poison-message DLQ;
  - tenant isolation and rate limiting (in-process and Redis);
  - the outbox, inbox and leader election.
- **Frontend tests:** components and pages, plus API integration through an MSW-mocked NEXUS API.

CI (`.github/workflows/ci.yml`) runs all of the above, plus offline `vercel build`s of both projects. On `main` it then migrates the production database, deploys, and verifies the deployment end to end.

---

## Deploying to Vercel

See **[docs/vercel-deployment.md](docs/vercel-deployment.md)**. In short:

| Component | Where it runs |
|---|---|
| Dashboard | Vercel project with Root Directory `frontend/` (Vite static build, SPA routing, security headers) |
| API | Vercel project at the repo root (Python Function, `vercel_app.api:app`) |
| Scheduler + RabbitMQ workers | **Vercel Daemons** (`deploy/vercel.daemons.json`; public beta, production deployments only) |
| PostgreSQL | Managed (e.g. Neon via the Vercel Marketplace) |
| Redis | Managed (e.g. Upstash via the Marketplace), optional |
| RabbitMQ | **External managed AMQP broker** (Vercel does not host RabbitMQ), e.g. CloudAMQP or Amazon MQ, over `amqps://` |

---

## Known limitations

- **Vercel Daemons are in public beta** and run only on production deployments. Preview deployments have an API but no scheduler or workers. The `/internal/maintenance` cron relays the outbox, but cannot execute jobs.
- **Vercel hosts no RabbitMQ.** A managed AMQP broker (for example CloudAMQP) is required in production.
- **Two broker-level dead-letter counts.** The broker's `nexus.dead-letter` count includes messages for jobs that were dead-lettered and later retried by hand. The dashboard's *Dead-lettered* figure counts jobs currently dead-lettered (`dead_lettered_at` set), from the database. Messages in `nexus.dead-letter` are kept for inspection and are not replayed automatically.
- **Rate limiting without Redis is per process.** Set `REDIS_URL` for limits shared across instances.
- **Tasks API retries over RabbitMQ.** The legacy `/tasks` DAG API retries through the scheduler's reconciler rather than outbox hold-back, so its retry delay is rounded up to the scheduler interval.
- **Fixed job types.** Workers run only registered handlers (`echo`, `http.webhook`, test types). Arbitrary code is never accepted.
- **No per-user identity.** Authentication is API-key per tenant; there are no user accounts or roles.
