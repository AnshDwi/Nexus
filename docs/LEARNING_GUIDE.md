# NEXUS learning guide

How NEXUS works, explained from the code. Every behaviour below names the file that implements it; open the file alongside the text. Paths are relative to `src/nexus/` unless they start with a top-level folder.

**Contents:**
1. What NEXUS does
2. Why this project exists
3. The problem it solves
4. Architecture overview
5. Why each technology
6. API request lifecycle
7. Job lifecycle
8. How a job enters RabbitMQ
9. How workers consume jobs
10. How PostgreSQL stores job state
11. How Redis is used
12. Scheduler responsibilities
13. Retry mechanism
14. Dead-letter queue
15. Idempotency and duplicate handling
16. Monitoring and metrics
17. Failure recovery
18. Docker networking
19. CI/CD workflow
20. Debugging

---

## 1. What NEXUS does

NEXUS runs **background jobs** reliably and shows exactly what happened to each one.
- **Submitting:** a client submits a job (a type, such as `echo` or `http.webhook`, and a JSON payload) through a REST API or the dashboard.
- **Running:** NEXUS stores it, queues it in RabbitMQ, and runs it on a worker process with a timeout.
- **Failures:** it retries failures with exponential backoff, and dead-letters jobs that can't succeed.
- **Record:** every state change and every attempt is recorded and visible in the dashboard and in metrics.

## 2. Why this project exists

It's a learning and portfolio project for the hard parts of backend engineering: delivering work between processes without losing or duplicating it, surviving crashes, and being able to explain what the system did. It isn't a commercial service. The production deployment work (`compose.production.yml`, `docs/PRODUCTION_DEPLOYMENT.md`) is kept as a learning resource.

## 3. The problem it solves

"Run this later" sounds simple, but naive implementations fail in specific ways:

| Failure | Naive approach | NEXUS |
|---|---|---|
| The job is saved but the queue message never sent (crash in between) | Write to DB, then publish | **Transactional outbox**: the job and its message are written in one DB transaction (§8) |
| Message published but the job isn't marked as such | Publish, then update DB | Marked QUEUED only after the **broker confirms** (§8) |
| The same message delivered twice | Run it twice | **Inbox** record per message (§15) |
| A worker dies mid-job | Job stuck "running" forever | **Heartbeats** + scheduler recovery (§17) |
| A slow or stuck job | Blocks forever | **Timeout** + execution lease (§9, §17) |
| A job that will never succeed | Retried forever | **Bounded retries**, then **dead-lettered** (§13, §14) |

## 4. Architecture overview

```
 Browser ─► Dashboard (React) ──/api──► API (FastAPI)
                                         │ one transaction: job row + outbox row
                                         ▼
                                    PostgreSQL ◄────────────── Scheduler (leader-elected)
                                         ▲    relays outbox, recovers jobs, prunes DLQ
                                         │
          ┌──────────── publish (confirmed) ──────────┘
          ▼
    RabbitMQ: exchange nexus.events ─► queue nexus.jobs ─► Worker(s): claim in DB, run handler, report to DB
                                                          (rejected) ─► nexus.dlx ─► nexus.dead-letter
    Redis: rate-limit counters (API only)
    Prometheus ◄─ scrapes API + daemons;  Grafana ◄─ reads Prometheus
```

**Code layout (Clean Architecture: dependencies point inward):**

| Layer | Folder | What's there |
|---|---|---|
| Domain | `domain/` | Pure rules, no I/O. `job.py` (Job state machine), `task_lifecycle.py` (`retry_backoff`), `leader_election.py` |
| Application | `application/use_cases/`, `application/interfaces/` | Use cases (`jobs.py`: `create_job`, `claim_job`, `report_success`, `report_failure`, `recover_jobs` …) and the ports (Protocols) they need: `JobStore`, `OutboxWriter`, `InboxRepository` |
| Infrastructure | `infrastructure/` | Implementations: `db/` (SQLAlchemy models and repositories), `messaging/` (RabbitMQ), `coordination/` (leader lease, worker registry), `execution/` (job executor, handlers, maintenance), `observability/` |
| Entry points | `api/`, `main.py`, `daemon.py` | FastAPI app (`main.py`, `api/v1/*`); the `scheduler` and `worker` processes (`daemon.py`) |

**Processes** (one Docker image, different commands in `docker-compose.yml`):
- `api`: `uvicorn nexus.main:app`;
- `scheduler`: `python -m nexus.daemon scheduler`;
- `worker`: `python -m nexus.daemon worker`;
- `migrate`: `alembic upgrade head`, once.

## 5. Why each technology

| Technology | Why | Where |
|---|---|---|
| **FastAPI** | Async I/O (the API mostly waits on the DB and broker), Pydantic validation, generated OpenAPI docs | `main.py`, `api/v1/` |
| **PostgreSQL** | The source of truth. Transactions make the outbox possible; `SELECT … FOR UPDATE [SKIP LOCKED]` gives safe concurrent claiming; partial unique indexes enforce idempotency | `infrastructure/db/` |
| **SQLAlchemy 2 (async) + asyncpg, Alembic** | Typed models, async sessions, versioned schema migrations | `infrastructure/db/models/`, `migrations/` |
| **RabbitMQ** | A real broker: per-message acknowledgements, redelivery when a consumer dies, publisher confirms, priority queues, dead-letter exchanges, `prefetch` for back-pressure | `infrastructure/messaging/` |
| **Redis** | Fast shared counters for rate limiting across API instances | `api/rate_limit.py` |
| **React + TypeScript + Vite + TanStack Query** | Typed UI; Query handles polling and caching (refresh every 3 s) | `frontend/src/` |
| **Prometheus + Grafana** | Pull-based metrics, alert rules, dashboards | `deploy/observability/` |
| **Docker Compose** | The whole distributed system on one laptop, with health-checked startup order | `docker-compose.yml` |

## 6. API request lifecycle

Take `POST /api/v1/jobs`, with route `create` in `api/v1/jobs.py`:

1. **Middleware** (`main.py`):
   - assigns `X-Request-ID`;
   - reads or creates `X-Correlation-ID` and stores it in a context variable (`correlation_id_var`), so every log line in this request carries it.
2. **Authentication** (`api/security.py`): the `Authorization: Bearer <key>` value is SHA-256-hashed and compared with the configured `API_KEYS` (`tenant:hash`). The result is a `Principal` with the `tenant_id`; a wrong or missing key gives 401.
3. **Rate limit** (`api/rate_limit.py`): a dependency counts requests per tenant per 60-second window, in Redis (`rl:write:<tenant>:<window>`), falling back to an in-process counter on Redis errors. Over the limit → 429 with `Retry-After`.
4. **Validation:** Pydantic validates the body (field types, `max_attempts` 1–20, payload size).
5. **Use case** `create_job` (`application/use_cases/jobs.py`):
   - checks the job type has a handler, and runs the admission rules (`domain/admission_rules.py`);
   - `store.insert_or_get_existing(job)`, which returns the existing job if this tenant already used the `Idempotency-Key`;
   - `request_job_dispatch(...)` writes the dispatch message to the **outbox** table;
   - all of this happens in **one transaction**, committed by the route.
6. **Eager relay:** after committing, the route calls `_relay()`, which runs `relay_pending_events` (§8) so the job is usually QUEUED before the response is sent.
7. **Response:** 201 with the job (200 for an idempotent replay).

Errors are shaped as `{"detail": {"code", "message"}}`. Unexpected 500s return only a `request_id`; internals stay in the logs.

## 7. Job lifecycle

The state machine lives in `domain/job.py`. Each transition is a method returning a new `Job` and a `JobTransition`, which is stored in `job_transitions`.

```
PENDING ──mark_queued──► QUEUED ──start──► RUNNING ──complete──► COMPLETED
   │                       ▲                  │
   │                       │                 fail
   │                  mark_queued             ▼
   │                       └──── RETRYING ◄── FAILED ──(exhausted or permanent)──► stays FAILED + dead_lettered_at
   └────────────── cancel (from PENDING/QUEUED/RETRYING/RUNNING) ──────────────► CANCELLED
```

- `DISPATCHABLE_JOB_STATES = {PENDING, RETRYING, QUEUED}`: a message can only start a job in one of these states.
- `fail()` either schedules a retry (FAILED → RETRYING with `next_attempt_at`) or, when `permanent` or `attempt >= max_attempts`, keeps FAILED and sets `dead_lettered_at`.
- `retry_manually()`: FAILED → RETRYING with a fresh budget (`max_attempts = attempt + retry_limit`).

## 8. How a job enters RabbitMQ

This is the **transactional outbox** pattern.

1. `request_job_dispatch` writes a `DomainEvent` (`job.dispatch_requested`, payload `{job_id, attempt}`) into `outbox_records`, in the same transaction as the job.
2. **A relay publishes it.** `relay_pending_events` (`infrastructure/messaging/relay.py`) repeatedly calls `run_relay_iteration` (`worker.py`) → `relay_outbox_once` (`application/use_cases/relay_outbox_once.py`), one transaction per message:
   1. `claim_next()` locks the next due row (`available_at <= now`) with `FOR UPDATE SKIP LOCKED`, so the API's eager relay and the scheduler's relay loop never take the same row.
   2. `JobDispatchHook.before_publish` (`use_cases/jobs.py`) **locks the job row** and skips the message if the job moved on (cancelled, or a newer attempt).
   3. `RabbitMQEventPublisher.publish` (`infrastructure/messaging/rabbitmq_publisher.py`) publishes to exchange `nexus.events`, routing key `job.dispatch_requested`. Messages are **persistent**, carry the job's priority (0–10), are **mandatory** (fail if no queue is bound), and the channel uses **publisher confirms**, so `publish` returns only after RabbitMQ has the message.
   4. `mark_dispatched`, then `JobDispatchHook.after_publish` moves the job PENDING → **QUEUED**, and the transaction commits.
3. **On failure:** if publishing fails (broker down), `record_failure` backs off the row (5 s, doubling, up to 1 h); the job stays PENDING and is retried later. Nothing is lost.

Topology (`infrastructure/messaging/rabbitmq_topology.py`): `nexus.jobs` is a durable queue with `x-max-priority: 10` and `x-dead-letter-exchange: nexus.dlx`.

## 9. How workers consume jobs

`RabbitMQJobWorker` (`infrastructure/messaging/rabbitmq_worker.py`) is started by `daemon.py worker`.

1. **Start:**
   - registers in `worker_registrations` (id, capacity, capabilities);
   - starts a heartbeat loop (every 5 s);
   - sets `prefetch_count = capacity`, so RabbitMQ never gives it more unacknowledged messages than it can run;
   - consumes `nexus.jobs`.
2. **For each message**, `_on_job_message` parses `{job_id, attempt}`. An unparseable message is rejected straight to the dead-letter queue. Otherwise it calls `JobExecutor.handle` (`infrastructure/execution/job_executor.py`).
3. **Claim** (`claim_job` in `use_cases/jobs.py`), in one transaction:
   1. insert an inbox row `job-executor:<message_id>`; if it already exists, the result is **DUPLICATE**, so ack and skip;
   2. lock the job row; if the attempt doesn't match or the state isn't dispatchable, the result is **STALE**, so ack and skip;
   3. otherwise `job.start(...)` → RUNNING, with `worker_id`, `started_at`, and a **lease** (`timeout + lease grace`). An execution row is created.
4. **Run:** the handler from `handlers.py` runs inside `asyncio.wait_for(..., job.timeout_seconds)`.
5. **Report:** `report_success` or `report_failure`, each guarded by `_live_attempt`, which checks that the job is still RUNNING with the same `attempt` **and** `worker_id`. A worker that was declared dead can't overwrite the retry's result; this is the fencing.
6. **Settle the message:**
   - `ack` when it completed, or failed with a retry scheduled (the retry is a *new* outbox message);
   - `reject` (→ dead-letter exchange) when the job ended FAILED;
   - `nack(requeue)` when the claim itself failed (for example, the DB was briefly down).

**Graceful shutdown** (`stop()`):
1. mark the worker draining;
2. stop consuming;
3. wait up to `WORKER_SHUTDOWN_GRACE_SECONDS` (25);
4. hand unfinished attempts back with `JobExecutor.abandon` (execution `lost`, job RETRYING);
5. deregister.

## 10. How PostgreSQL stores job state

Models are in `infrastructure/db/models/`; the schema is in `migrations/versions/`.

| Table | Purpose |
|---|---|
| `jobs` | Current state of each job. Key columns: `state`, `attempt`, `max_attempts`, `retry_limit`, `next_attempt_at`, `dead_lettered_at`, `worker_id`, `lease_expires_at`, `last_error`, `result`, `idempotency_key` (partial unique index `uq_jobs_tenant_idempotency_key` on `(tenant_id, idempotency_key)`), `version` |
| `job_transitions` | Append-only history: `from_state`, `to_state`, `attempt`, `reason`, `occurred_at` (the dashboard timeline) |
| `job_executions` | One row per attempt: `worker_id`, `status` (running/completed/failed/timed_out/lost/cancelled), `duration_ms`, `error` |
| `outbox_records` | Messages waiting to be published: `available_at`, `dispatched_at`, `attempts`, `failed_permanently_at` |
| `inbox_records` | Processed message keys, for deduplication |
| `worker_registrations` | Workers, capacity, `last_heartbeat_at`, `draining` |
| `scheduler_leader_lease` | Leader election: holder, `expires_at`, `fencing_token` |
| `tasks`, `task_transitions` | The separate `/api/v1/tasks` DAG feature (§12, and the interview guide) |

Concurrency is handled with row locks (`get_for_update` → `SELECT … FOR UPDATE`) inside short transactions, and time comes from the database clock (`store.now()`), so every process agrees on time.

## 11. How Redis is used

Only for **rate limiting** (`api/rate_limit.py`):
- `INCR rl:<read|write>:<tenant>:<minute-window>` with a 65-second expiry;
- limits come from `RATE_LIMIT_READS_PER_MINUTE` and `RATE_LIMIT_WRITES_PER_MINUTE`.

Redis holds **no job state**. If Redis is unreachable, the limiter falls back to an in-process counter, so limits become per API process. `/api/v1/ready` reports Redis as failing, but jobs keep working.

## 12. Scheduler responsibilities

`daemon.py scheduler` runs two loops:
- **Relay loop**, every 1 s (`OUTBOX_RELAY_POLL_INTERVAL_SECONDS`), on every replica: `relay_pending_events`. This is what turns RETRYING jobs back into QUEUED once their backoff ends.
- **Maintenance pass**, every 5 s (`DAEMON_TICK_INTERVAL_SECONDS`): `run_maintenance_pass` (`infrastructure/execution/maintenance.py`).

**The maintenance pass:**
1. **Leader election** (`infrastructure/coordination/leader_election.py`): one atomic `INSERT … ON CONFLICT DO UPDATE … WHERE` acquires or renews a 15-second lease. Each new term increments the **fencing token**. Only the leader continues.
2. **Job recovery** (`_recover_jobs_fenced` → `recover_jobs`), in a transaction that re-checks the fencing token with `holds_lease` (`SELECT … FOR SHARE`), so a paused former leader can't commit:
   - workers whose heartbeat is older than `WORKER_HEARTBEAT_TTL_SECONDS` (30) are dead: their RUNNING jobs fail (`lost`) and retry;
   - RUNNING jobs past their lease (`timeout + grace`) fail (`timed_out`) and retry;
   - jobs stuck PENDING, QUEUED or RETRYING without a dispatch for `JOB_REDISPATCH_AFTER_SECONDS` (900) get a new dispatch message (a lost broker message, for example).
3. **Task reconciler** (`use_cases/reconcile_tasks.py`), for the DAG task feature: promotes tasks whose dependencies completed, cancels tasks blocked by a failed dependency, and expires leases.
4. **Dead-letter pruning:** after the pass, the leader calls `prune_resolved_dead_letters` (`infrastructure/messaging/dead_letter.py`), see §14.

## 13. Retry mechanism

- **Backoff:** `retry_backoff` (`domain/task_lifecycle.py`) = `initial_backoff_seconds × 2^(attempt−1)`, capped at `max_backoff_seconds`.
- **Scheduling the retry:** `Job.fail()` sets RETRYING, `attempt + 1`, and `next_attempt_at = now + backoff`. `_fail_running` (`use_cases/jobs.py`) then writes a new outbox message with `deliver_after = next_attempt_at`; the outbox stores it as `available_at` (`infrastructure/db/repositories/outbox_repository.py`).
- **Holding it back:** the relay doesn't publish it until `available_at`, so the delay lives in PostgreSQL. No RabbitMQ delay plugin is needed, and the delay survives broker restarts.
- **Permanent errors:** a handler raising `PermanentTaskError`, or a result that isn't JSON-serializable, is never retried.
- **Manual retry:** `POST /jobs/{id}/retry` (`retry_job`) and `POST /jobs/bulk-retry` (`retry_jobs`, up to 100, with an optional error filter) work only on FAILED jobs.

## 14. Dead-letter queue

There are two related things:
- **In the database:** a job that ends FAILED (exhausted or permanent) gets `dead_lettered_at` and keeps its full history. This is what the dashboard's *Dead-lettered* filter shows (`GET /jobs?dead_lettered=true&error=…`).
- **In the broker:** the worker `reject`s that message, so RabbitMQ routes it through `nexus.dlx` into `nexus.dead-letter`. Poison (unparseable) messages land there too.

When a dead-lettered job is retried, its old broker message is stale. The leader runs `prune_resolved_dead_letters` each pass:
1. it takes up to 200 messages with `basic.get` (unacknowledged);
2. looks their jobs up;
3. **acks** (deletes) only job messages whose job is no longer dead-lettered for that attempt;
4. **nacks back** everything else: messages for jobs still dead-lettered, task messages, poison messages, unknown jobs.

So the broker count matches the DB count, and nothing unexplained is ever deleted.

## 15. Idempotency and duplicate handling

There are three layers, each for a different duplicate:

1. **Duplicate submissions.** The client sends `Idempotency-Key`; the partial unique index on `(tenant_id, idempotency_key)` plus `insert_or_get_existing` return the original job (HTTP 200) instead of creating a second one.
2. **Duplicate deliveries.** RabbitMQ is at-least-once: a message can be redelivered after a connection drop, and the relay can republish if it crashes after the broker confirm but before its commit. Each message has a `message_id` (the outbox `event_id`); `claim_job` first inserts `inbox_records` key `job-executor:<message_id>`, and a second delivery is a no-op.
3. **Stale messages.** A message names an `attempt`. If the job has moved on (retried, cancelled, already running), `claim_job` returns STALE. Results are accepted only from the live `attempt` + `worker_id`.

**What isn't guaranteed:** *state transitions* happen once, but a *handler* can run more than once for the same job. Example: a worker runs a webhook, then dies before reporting; the scheduler retries; the webhook runs again (attempt 2). Handlers with external side effects should be idempotent themselves; that's normal for at-least-once systems.

## 16. Monitoring and metrics

- **Process counters** (`infrastructure/observability/metrics.py`, `prometheus_client`): job executions by type and outcome, relay counts, recoveries, leadership transitions, and so on. Each daemon serves its own on `:8081/metrics` (`daemon.py serve_health`).
- **Fleet metrics computed from durable state** (`api/v1/state_metrics.py`) on every scrape of `GET /api/v1/internal/metrics` (bearer `METRICS_TOKEN`):
  - `nexus_jobs{tenant,state}`;
  - `nexus_jobs_dead_lettered`;
  - `nexus_jobs_oldest_waiting_seconds`;
  - `nexus_job_executions_total`;
  - durations;
  - outbox lag;
  - workers and slots;
  - broker queue depths.

  These are read from PostgreSQL and RabbitMQ, so they are correct no matter which process or instance answers.
- **Prometheus** (`deploy/observability/prometheus/prometheus.yml`) scrapes both; there are **11 alert rules** in `alerts.yml`, with unit tests in `alerts.test.yml`.
- **Grafana** auto-provisions the *NEXUS — Job platform* dashboard (`deploy/observability/grafana/`).
- **Logs** are JSON (`infrastructure/observability/logging.py`) with `request_id`, `correlation_id`, `job_id` and `attempt`.

## 17. Failure recovery

| What fails | What happens | Mechanism |
|---|---|---|
| API crashes | Committed jobs are safe; uncommitted requests get an error, and the client retries with the same `Idempotency-Key` | DB transactions, idempotency |
| RabbitMQ down | API still accepts jobs (PENDING in the outbox); `/ready` → 503; jobs are published when it returns | Outbox + relay backoff |
| PostgreSQL down | API requests fail (`/ready` → 503); workers can't claim, so they nack messages back to the queue; the daemons' health checks turn unhealthy (visible in `docker compose ps`). Docker restarts a container only if it *exits*, so the processes keep retrying and resume when the DB returns (verified by the PostgreSQL-restart drill) | Short transactions, requeue, retry loops |
| Worker crashes mid-job | The unacked message returns to the queue. Another worker receives it, but `claim_job` finds its inbox key already recorded (→ DUPLICATE), so it doesn't run the job again. After 30 s without a heartbeat, the scheduler fails the attempt (`lost`) and dispatches a retry as a new message | Heartbeats, inbox, `recover_jobs`, fencing |
| Job hangs | `asyncio.wait_for` timeout; if the whole worker hangs, lease expiry | Timeout + lease |
| Scheduler crashes | Another replica takes the lease after 15 s; a stale leader is fenced out | Leader lease + fencing token |
| Redis down | Rate limiting falls back to in-process counters | `rate_limit.py` |
| Broker message lost | Re-dispatched after 15 min | `stale_dispatch_for_update` |

These scenarios are exercised by `tests/integration/test_job_pipeline_rabbitmq.py` and the production drills in `tests/e2e/test_production_resilience.py` (which restart and kill real containers).

## 18. Docker networking

- **The network:** Compose creates one network, `nexus_default`. Each service is reachable by name through Docker's DNS: the API connects to `postgres:5432`, `rabbitmq:5672` and `redis:6379` (`x-app-env` in `docker-compose.yml`), and the dashboard proxies `/api` to `http://api:8000` (`frontend/vite.config.ts`).
- **Host ports** are published only on `127.0.0.1` (`ports: "127.0.0.1:${API_HOST_PORT:-8000}:8000"`), so your machine can reach them but your network can't.
- **Inside vs outside:** `localhost` inside a container means that container itself. That's why `.env` says `POSTGRES_HOST=localhost`, for tools you run on the host, while compose sets `POSTGRES_HOST=postgres` for containers.
- **Production** (`compose.production.yml`) publishes only Caddy (80/443).

## 19. CI/CD workflow

`.github/workflows/ci.yml`, on pull requests and pushes to `main`:

| Job | What it proves |
|---|---|
| `test` | ruff, format, mypy, unit tests |
| `integration` | integration tests against real PostgreSQL, RabbitMQ and Redis service containers |
| `frontend` | typecheck, lint, Vitest, build |
| `vercel-build` | the API builds for Vercel |
| `compose-e2e` | the local stack from scratch: e2e tests, demo seed, alert-rule tests, Grafana provisioning, graceful shutdown |
| `production-config` | the production compose renders, and refuses to without secrets; shellcheck; Caddyfile; promtool |
| `production-stack` | the production stack on official images: deploy, HTTPS smoke test, 10 failure drills, backup/restore |
| `deploy-production` | only on `main`: deploys to Vercel (needs secrets) |

`.github/workflows/pages.yml` builds the dashboard in demo mode for GitHub Pages (`docs/github-pages-demo.md`), and deploys only from `main`.

## 20. Debugging

1. **Is everything up?** Run `docker compose ps`, then `curl localhost:8000/api/v1/ready`, which names any failing dependency.
2. **Follow one job.** The dashboard's job detail shows transitions and attempts. Or in SQL:
   ```sql
   SELECT from_state, to_state, attempt, reason, occurred_at FROM job_transitions WHERE job_id = '…' ORDER BY id;
   SELECT attempt, worker_id, status, duration_ms, error FROM job_executions WHERE job_id = '…';
   ```
3. **Logs by correlation:** take `X-Correlation-ID` from the API response, then `docker compose logs api worker | grep <correlation-id>`.
4. **Stuck PENDING?**
   ```sql
   SELECT event_id, attempts, last_error, available_at FROM outbox_records WHERE dispatched_at IS NULL;
   ```
   Is RabbitMQ up? Is the scheduler running?
5. **Stuck QUEUED?** `rabbitmqctl list_queues name messages consumers`: are there 0 consumers? Check the *Workers* page.
6. **Stuck RUNNING?** Check `lease_expires_at` and the worker's `last_heartbeat_at`; the scheduler recovers it once the lease or heartbeat expires.
7. **Metrics:** Grafana at :3000, or Prometheus at :9090 for queries such as `nexus_outbox_lag_seconds`.
8. **Breakpoints:** run one component on the host instead of in Docker. Stop the container, then run the same command with `uv run` (for example `uv run python -m nexus.daemon worker`); `.env` points it at the containerized infrastructure on `localhost`.
