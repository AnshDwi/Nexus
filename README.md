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

---

## Quick start

### With Docker Compose

```bash
cp .env.example .env
docker compose up --build
```

Then open **http://localhost:5173** and connect with the local API key `dev-key`.

Compose starts:
- `postgres`, `rabbitmq` (management UI at :15672, `nexus`/`nexus`) and `redis`;
- `migrate`, `api` (:8000), `scheduler`, `worker` and `dashboard` (:5173).

### Without Docker

Requires Python 3.12 + [uv](https://docs.astral.sh/uv/), Node 22, and PostgreSQL 16, RabbitMQ 3.12+ and Redis running locally.

```bash
uv sync && cp .env.example .env
uv run alembic upgrade head
uv run uvicorn vercel_app.api:app --reload                    # API on :8000
uv run python -m nexus.daemon scheduler                       # relay + recovery
DAEMON_HEALTH_PORT=8082 uv run python -m nexus.daemon worker  # RabbitMQ worker
cd frontend && npm ci && npm run dev                          # dashboard on :5173
```

### Try it

The dashboard lets you:
- **Create a job:** Jobs → Create job.
- **Watch it move:** PENDING → QUEUED → RUNNING → COMPLETED, refreshed every 3 s.
- **See a failure and retry it:** submit a `test.flaky` job with payload `{"succeed_on_attempt": 3}` and *Max attempts* 2. It fails twice and is dead-lettered. Click **Retry** and it completes on attempt 3.

From the command line:

```bash
curl -s -X POST localhost:8000/api/v1/jobs \
  -H "Authorization: Bearer dev-key" -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"name":"hello","job_type":"echo","payload":{"msg":"hi"},"priority":"high"}'
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
| `GET` | `/jobs` | List with pagination: `page`, `page_size` (≤100), `status` (repeatable), `priority` (repeatable), `q` (name/type/ID search) |
| `GET` | `/jobs/{id}` | Job, state transitions and execution attempts |
| `POST` | `/jobs/{id}/cancel` | Cancel PENDING/QUEUED/RETRYING/RUNNING (409 otherwise). A running attempt's result is discarded |
| `POST` | `/jobs/{id}/retry` | Retry a FAILED job with a fresh attempt budget (409 otherwise) |
| `GET` | `/workers` | Registered workers: status (healthy/unhealthy/draining), heartbeat age, running jobs vs capacity |
| `GET` | `/queue` | Pending/queued/retrying jobs by priority with dispatch status; broker ready and dead-letter counts |
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
```

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
