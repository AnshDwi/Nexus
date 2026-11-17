# Deploying NEXUS on Vercel

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
| FastAPI API (health, tasks, cron, metrics) | Vercel Function | Entrypoint `vercel_app.api:app` (path shim for `src/` layout) |
| Task executor | Vercel Queues push consumer (private function, generated from `[[tool.vercel.subscribers]]`) | Topic `nexus-task-dispatch`, group `nexus-executor`, `retry_after=15s` |
| Outbox relay | Inside the API/consumer invocations (eager), Cron sweep, optional Daemon | All concurrent-safe (SKIP LOCKED + queue idempotency key) |
| Reconciler + leader lease | Cron sweep and/or `scheduler` Daemon | Lease `nexus-maintenance`, database clock, fencing token |
| Long-running poll worker | Optional Daemon (`python -m nexus.daemon worker`) | Use *instead of* push mode, not alongside it |
| PostgreSQL | Marketplace provider | Migrations run from CI before deploy |
| RabbitMQ adapter | Not on Vercel | Kept for `EVENT_BUS=rabbitmq` self-hosting |
| Redis | Optional Marketplace provider | `REDIS_URL` enables it; readiness only checks it when configured |
| Worker registry (heartbeats) | Unused by the Vercel pipeline | Vercel scales consumers itself; kept for self-hosted workers |

### Request and task flow

```
POST /api/v1/tasks ─► [tx] tasks + task_transitions + outbox(dispatch) ─► commit
                     └► eager relay ─► Vercel Queues "nexus-task-dispatch" (idempotency key = event_id)
                                            │ push
                                            ▼
            queue consumer function: claim (attempt-guarded) ─► handler (timeout) ─► [tx] record
                                            └► eager relay of follow-ups (retry / dependents)
Cron (and/or scheduler Daemon) ─► leader lease ─► reconcile ─► relay backlog
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

1. **Create the project.** Import the repo in Vercel. `vercel.json` pins
   `"framework": "fastapi"`. Pin the Functions region to match your database.
2. **Add Postgres.** Install Neon (or another provider) from the Marketplace
   and connect it to Production (and Preview, if previews should work). This
   injects `DATABASE_URL` and `DATABASE_URL_UNPOOLED`. A `-pooler` host is
   auto-detected and the asyncpg statement cache is disabled for PgBouncer.
3. **Enable Queues** for the team. Push consumers authenticate with OIDC
   automatically.
4. **Set environment variables.** Mark secrets as Sensitive.

   | Variable | Value |
   |---|---|
   | `API_KEYS` | `tenant:sha256,...` (`uv run python -m nexus.cli new-api-key <tenant>`) |
   | `CRON_SECRET` | random, ≥ 32 characters |
   | `METRICS_TOKEN` | optional |
   | `VERCEL_QUEUE_REGION` | e.g. `iad1`, same as the function region |
   | `WEBHOOK_ALLOWED_HOSTS` | optional; enables the `http.webhook` task type |
   | `TASK_ENABLE_TEST_HANDLERS` | `false` in production |

   The app refuses to start outside local/test if credentials are defaults,
   `API_KEYS` is empty, or `DEBUG` is on.
5. **Migrate before deploy.** CI runs `alembic upgrade head` against
   `DATABASE_URL_UNPOOLED`, then `vercel build --prod` and
   `vercel deploy --prebuilt --prod`, then
   `python -m nexus.cli verify-deployment <url>`. Migrations must stay
   backward compatible with the previous deployment: messages pinned to it
   keep being consumed after a promote.
6. **Choose a recovery cadence.**
   - **Hobby:** the default daily cron, plus queue redelivery and eager relay.
   - **Pro:** use `deploy/vercel.pro-cron.json` (every minute).
   - **Seconds-level recovery:** use `deploy/vercel.daemons.json`. It runs two
     `scheduler` daemon instances (beta, production only). Copy the file over
     `vercel.json`. If `VERCEL_DEPLOYMENT_ID` is not present in daemon
     instances, the daemon only reconciles and leaves publishing to the
     Functions (it logs a warning).

### Preview deployments

Previews get Functions, Queues and the API, but **no daemons and no cron**.
Eager relay and queue redelivery still execute tasks. Repairs only happen when
`/api/v1/internal/maintenance` is called manually.

Use a per-preview Neon branch, or a separate database. If you do, run
migrations from `[tool.vercel.scripts] build` for previews only.

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
