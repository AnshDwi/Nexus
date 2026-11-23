# Deploying NEXUS on Vercel

> Updated for the job pipeline (ADR 0005): RabbitMQ is the default bus;
> Vercel Queues remains an opt-in transport.

This document covers:
1. Vercel capabilities and limits relevant to NEXUS (researched November 2026).
2. How each NEXUS component maps onto them.
3. The delivery guarantees and how they were verified.
4. Setup, operations and the remaining gaps.

## 1. Vercel capabilities used (and their documented limits)

| Capability | Status | What NEXUS uses it for | Limits that matter |
|---|---|---|---|
| Python runtime / FastAPI on Functions (Fluid compute) | GA | API (`vercel_app.api:app`) | Lifespan events supported; one instance serves concurrent invocations (pooling is safe); default/max duration 300s on Fluid; bundle ≤ 500 MB |
| Vercel Queues (push mode, Python SDK `vercel-queue`) | **Public beta** | Task dispatch + execution | At-least-once; default visibility 60s, extendable; retention 60s–7d (default 24h); delay ≤ 7d; publish idempotency keys dedupe for the retention window; no built-in DLQ; approximate ordering; topics partitioned **per deployment** |
| Cron Jobs | GA | Maintenance sweep (`GET /api/v1/internal/maintenance`, `Authorization: Bearer $CRON_SECRET`) | Hobby: once/day (±59 min). Pro/Enterprise: once/minute. Production deployments only |
| Daemons | **Public beta** | Optional `scheduler` loop (seconds-level recovery); optional poll-mode `worker` for long tasks | **Production deployments only** (no previews, not in `vercel dev`); fixed instance counts; ephemeral storage; SIGTERM + grace period; HTTP liveness probe |
| Marketplace Postgres (e.g. Neon) | GA | System of record | Injects `DATABASE_URL` (pooled) / `DATABASE_URL_UNPOOLED`; per-preview branches optional |
| Marketplace Redis (e.g. Upstash) | GA | Optional only | Nothing in the pipeline needs Redis |

**What Vercel does not do:**
- It does not run `docker-compose.yml` services.
- It does not host RabbitMQ.
- It must not run infinite loops inside request functions.

The NEXUS changes are shaped around these three constraints.

## 2. Component mapping

| NEXUS component | Runs on | Notes |
|---|---|---|
| **Dashboard** (React/Vite) | Vercel project #2, Root Directory `frontend/` | Static build. `frontend/vercel.json` adds the SPA fallback and security headers. Set `VITE_API_BASE_URL` to the API project URL |
| **API** (FastAPI) | Vercel project #1, repo root | Python Function, entrypoint `vercel_app.api:app` (path shim for the `src/` layout). Set `CORS_ALLOWED_ORIGINS` to the dashboard origin |
| **Scheduler** (relay loop, fenced recovery, dead-worker detection) | Vercel **Daemon** `scheduler` | `deploy/vercel.daemons.json`. Two instances; one leads via the PostgreSQL lease |
| **Job workers** (RabbitMQ consumers) | Vercel **Daemon** `worker` | Same image, `python -m nexus.daemon worker`. Registration, heartbeats, DLQ, SIGTERM drain (grace 60 s) |
| **RabbitMQ** | **External managed broker** (CloudAMQP, Amazon MQ, self-run) | Vercel does not host RabbitMQ. Daemons connect outbound over `amqps://` (`RABBITMQ_URL`). The API Function opens one connection per instance for confirmed publishes |
| **PostgreSQL** | Marketplace (e.g. Neon) | `DATABASE_URL` (pooled) + `DATABASE_URL_UNPOOLED` (migrations) |
| **Redis** | Marketplace (e.g. Upstash, TCP `rediss://`), optional | Shared rate-limit counters (`REDIS_URL`) |
| **Cron** | Vercel Cron → `/api/v1/internal/maintenance` | Backstop scheduler pass (daily on Hobby, every minute on Pro) |
| Vercel Queues adapter | Optional (`EVENT_BUS=vercel_queues`) | Push-mode consumers for tasks *and* jobs. An alternative transport, **not** the default; RabbitMQ is never replaced silently |

### What runs without Daemons

Previews, Hobby without Daemons, or Daemons not yet enabled:
- The API, the dashboard, persistence, dispatch (confirmed publish to
  RabbitMQ) and the daily cron all work.
- **Execution does not.** Nothing consumes `nexus.jobs` until a worker is
  connected to the same RabbitMQ and Postgres. A worker on any container host
  works, but it is not the documented primary path.

Jobs simply wait in QUEUED; none are lost.

### Request and job flow

```
Dashboard ─► POST /api/v1/jobs ─► [tx] jobs + job_transitions + outbox ─► commit
                                 └► relay: lock job, publish (confirm) ─► [same tx] QUEUED
RabbitMQ nexus.jobs ─► worker daemon: inbox + claim (RUNNING) ─► handler ─► [tx] COMPLETED |
                       FAILED→RETRYING (+ delayed outbox dispatch) | FAILED (dead-lettered → nexus.dead-letter)
Scheduler daemon (leader, fenced) ─► relay due retries · recover dead-worker / expired-lease jobs
```

## 3. Guarantees and how they were verified

| Guarantee | Mechanism | Verified by |
|---|---|---|
| Committed task is never lost | Task + dispatch request in one transaction; relay retries with backoff; sweep | e2e `test_task_committed_while_queue_unreachable…`; smoke run where publishing failed for ~1 min, then everything drained |
| One live worker per attempt | Row-locked, attempt-guarded claim | `test_concurrent_claims…` (10 concurrent claims → exactly 1) |
| Duplicate delivery runs the task once | Claim guard | e2e `test_duplicate_deliveries_execute_the_task_once` |
| Worker crash mid-task is retried | Lease expiry → reconciler or redelivered claim | e2e + smoke (`kill -9` worker during a 6 s task → attempt 2 succeeded) |
| Stale worker cannot overwrite | Result recorded only for the live attempt | `test_expired_lease_fails_attempt_and_fences_the_old_worker` |
| Retry/backoff, permanent failure, cascade | Domain `plan_failure` + `cancel_dependents` | unit + e2e |
| Dependencies promoted exactly once under concurrency | Dependents locked before reading other deps | 5-round race test |
| Single active reconciler; failover | PostgreSQL lease (DB time) + fencing token | existing ADR 0003 tests + smoke (killed leader; standby took over with token 94→95) |
| Tenant isolation | API keys bound to tenants; every query scoped | e2e `test_authentication_and_tenant_isolation` |

### Test layers

- **Unit tests:** run with no infrastructure.
- **Integration tests:** need Postgres. They use the `vercel-queue` SDK's embedded queue service, which runs the real client, lease, push, acknowledge and retry code in-process.
- **Smoke test:** separate processes:
  - the API from Vercel's own build venv, with `VERCEL_ENV=production`;
  - the SDK's standalone queue devserver;
  - scheduler and worker daemons.

### Not verified (requires a real Vercel project)

- An actual deployment.
- The hosted Queues service: auth, OIDC, deployment partitioning in production.
- Daemon image build/push: the schema was accepted by `vercel build`, but the registry push needs a linked project.
- Cron invocation in production.

## 4. Setup

1. **Provision managed services:**
   - Postgres: Neon from the Vercel Marketplace.
   - Redis (optional): Upstash from the Marketplace.
   - A RabbitMQ broker reachable over TLS, e.g. CloudAMQP. Copy its `amqps://` URL.
2. **API project** (repo root; `vercel.json` pins `"framework": "fastapi"`).
   Set these environment variables, marking secrets as Sensitive:

   | Variable | Value |
   |---|---|
   | `RABBITMQ_URL` | `amqps://…` |
   | `API_KEYS` | `tenant:sha256,…` (`uv run python -m nexus.cli new-api-key <tenant>`) |
   | `CRON_SECRET` | random value |
   | `CORS_ALLOWED_ORIGINS` | `https://<dashboard>.vercel.app` |
   | `REDIS_URL` | optional |
   | `METRICS_TOKEN` | optional |
   | `WEBHOOK_ALLOWED_HOSTS` | optional |

   `DATABASE_URL` and `DATABASE_URL_UNPOOLED` come from the Marketplace.
   Keep `TASK_ENABLE_TEST_HANDLERS=false` in production.
3. **Enable execution.** Copy `deploy/vercel.daemons.json` over `vercel.json`
   (adds the `scheduler` and `worker` daemons) and deploy to production.
   Daemons read the same environment variables. Scale with
   `vercel daemons` or the dashboard.
4. **Dashboard project.** Import the same repository again with Root Directory
   `frontend`. Set `VITE_API_BASE_URL=https://<api-project>.vercel.app`. Users
   enter their API key in the dashboard; it is stored only in their browser.
5. **Migrate before deploying.** The CI job `deploy-production` runs
   `alembic upgrade head` against `DATABASE_URL_UNPOOLED`, then
   `vercel deploy --prebuilt --prod`, then `nexus.cli verify-deployment`.
   Migrations must be backward compatible with the running version.
6. **Verify:**

   ```
   uv run python -m nexus.cli verify-deployment https://<api>.vercel.app --api-key <key>
   ```

   This checks health, readiness and auth, then submits a task and waits for
   it to execute. Then open the dashboard and submit a job.

## 5. Operations

- **Health:**
  - `GET /api/v1/health` is liveness.
  - `GET /api/v1/ready` returns 503 if Postgres (or a configured Redis/RabbitMQ) fails.
  - Daemons serve `GET :8081/health` for the liveness probe.
- **Logs:** one JSON object per line on stdout, including `request_id`
  (`x-vercel-id`), `task_id` and `attempt`. Forward them with a Vercel log drain.
- **Queue health:** Vercel dashboard → Observability → Queues (backlog,
  retries). Set a retry-depth alert.
- **Task health (SQL):**

  ```sql
  SELECT state, count(*) FROM tasks GROUP BY 1;
  SELECT count(*) FROM outbox_records WHERE dispatched_at IS NULL AND failed_permanently_at IS NULL;
  SELECT * FROM outbox_records WHERE failed_permanently_at IS NOT NULL;  -- dead-lettered publishes
  ```

  A dead-lettered dispatch is still recovered by the reconciler, which
  re-dispatches stale READY tasks.
- **Metrics:** `GET /api/v1/internal/metrics` with `METRICS_TOKEN`. Counters
  are per instance on Functions.

## 6. Known gaps

- **Task duration:** bounded by the consumer's `maxDuration`. For longer
  tasks, switch to the poll-mode worker daemon.
- **Cancelling RUNNING tasks:** marks them CANCELLED and discards the result,
  but does not interrupt the handler.
- **Task types:** only registered handlers run (`echo`, `http.webhook` with an
  allow-list, and test handlers). Adding a type is a code change.
- **Scheduling policies:** EDF, fair share and resource matching are still
  domain-only. The pipeline is FIFO-by-arrival.
- **API keys:** stored as hashed env vars. Rotation means a redeploy, and
  there is no per-key revocation API.
- **Rate limiting:** none. Use Vercel Firewall rules, or add Redis-based limits.

## Sources

- [Vercel Queues concepts](https://vercel.com/docs/queues/concepts)
- [Queues Python SDK](https://vercel.com/docs/queues/python-sdk)
- [Daemons](https://vercel.com/docs/daemons)
- [FastAPI on Vercel](https://vercel.com/docs/frameworks/backend/fastapi)
- [Python runtime](https://vercel.com/docs/functions/runtimes/python)
- [Cron usage & pricing](https://vercel.com/docs/cron-jobs/usage-and-pricing)
- [Connection pooling with Fluid compute](https://vercel.com/kb/guide/efficiently-manage-database-connection-pools-with-fluid-compute)
- [Marketplace storage](https://vercel.com/docs/marketplace-storage)
