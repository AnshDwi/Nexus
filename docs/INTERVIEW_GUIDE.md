# NEXUS interview guide

Questions you're likely to get about NEXUS, with answers grounded in the code. **Implemented** answers cite the file. Anything marked **🔮 Not implemented** is a design idea you can discuss, but shouldn't claim as built. For the mechanics behind each answer, see [LEARNING_GUIDE.md](LEARNING_GUIDE.md).

**The 30-second pitch:** "NEXUS is a background-job platform I built to learn distributed-systems reliability:
- jobs go through a FastAPI API into PostgreSQL;
- a transactional outbox publishes them to RabbitMQ with publisher confirms;
- workers claim each delivery through an inbox table and run it with a timeout;
- failures retry with exponential backoff and end up dead-lettered;
- a leader-elected scheduler with fencing tokens recovers jobs from dead workers, but only judges workers it could actually observe;
- workers fence themselves when they lose PostgreSQL, so a job is never run by two workers at the same time.

It has a React dashboard, Prometheus metrics computed from durable state, and failure drills that kill real containers or cut workers off from the database mid-job and check that every job finishes with exactly one completed execution and no overlapping attempts. Handlers are at-least-once; I don't claim exactly-once side effects."

---

## Architecture

**Q: Walk me through what happens when I submit a job.**
1. `POST /api/v1/jobs` (`api/v1/jobs.py`): authenticate the API key, rate-limit, validate.
2. `create_job` (`application/use_cases/jobs.py`) inserts the job (PENDING) **and** an outbox message in one transaction.
3. After commit, the request eagerly runs the relay (`relay_pending_events`). That publishes to RabbitMQ with publisher confirms and, in the same transaction, marks the job QUEUED.
4. A worker receives it:
   - `claim_job` records the message in the inbox, locks the job, and moves it to RUNNING;
   - the handler runs with a timeout;
   - `report_success` → COMPLETED.

**Q: Why the layered (Clean Architecture) structure?**
- `domain/` holds the state machine with no I/O, so it's unit-tested in isolation (`tests/unit/test_job_domain.py`).
- Use cases depend on Protocols (`application/interfaces/`), not SQLAlchemy or aio-pika.
- That let the same job logic run behind two transports: RabbitMQ, and Vercel Queues (`EVENT_BUS`).

**Q: Why is the API a separate process from the workers?**

They scale and fail independently. The API is latency-sensitive and stateless; workers are long-running and resource-heavy. The scheduler is a third process because recovery must happen even if no request arrives.

**Q: What is the source of truth: the database or the queue?**

PostgreSQL. The queue only carries "please run attempt N of job X". If a broker message is lost, the scheduler re-dispatches from the DB (`stale_dispatch_for_update`, after `JOB_REDISPATCH_AFTER_SECONDS` = 900). If a stale message arrives, the DB state makes the worker ignore it.

## Backend

**Q: How is authentication done? Authorization?**
- **Authentication:** `Authorization: Bearer <key>`. Keys are stored only as SHA-256 hashes in `API_KEYS` (`tenant:hash`), compared in `api/security.py`.
- **Authorization:** every key belongs to one tenant, and every query filters by `tenant_id`; another tenant's job returns 404 (integration-tested).
- **Limitations:** there are no roles; any key can do everything within its tenant. Production refuses to start with the public `dev-key` (`infrastructure/config.py`).

**Q: How do you validate input and report errors?**
- Pydantic models on each route.
- Domain errors map to 404/409/422 with `{"detail": {"code", "message"}}`.
- Unexpected errors return 500 with only a `request_id`; details go to the logs.

**Q: How do you trace a request?**

Middleware in `main.py` sets `X-Request-ID` and `X-Correlation-ID`, stored in a context variable that the JSON log formatter adds to every line. The job stores the correlation ID, so the worker's log lines for that job carry it too.

## Distributed systems

**Q: Can a job execute more than once?**

**Yes, the handler can, one run after another. Never two at once. State transitions can't repeat.**
- **State transitions are effectively-once.** Duplicate *deliveries* are dropped by the inbox (`claim_job` → DUPLICATE). Stale messages are dropped by attempt checks (→ STALE). Results from a superseded worker are rejected (`_live_attempt` checks `attempt` + `worker_id` under the row lock).
- **No concurrent duplicates.** A worker that loses PostgreSQL stops its handlers 20 s after its last good heartbeat (self-fencing), before the scheduler can declare it dead at 30 s; the scheduler doesn't declare anyone dead until it has watched heartbeats for a full TTL (observation window). Proven by drills that cut one worker off and record each handler's start and end.
- **Sequential duplicates remain: the handler is at-least-once.** If a worker runs the handler and dies before reporting, the scheduler retries, and the handler runs again as attempt 2. Exactly-once external side effects aren't possible without the receiver's cooperation. Handlers get `TaskContext.idempotency_key`, the job ID, which is the same for every attempt; `http.webhook` sends it as `Idempotency-Key`. (It must not include the attempt number, or every retry would look new to the receiver.)

**Q: What happens if a worker crashes after receiving a job?**

Two cases:
- **Before the claim commits:** the message is unacked, so RabbitMQ redelivers it to another worker, which claims normally.
- **After the claim (RUNNING):**
  1. RabbitMQ redelivers the unacked message;
  2. the next worker's `claim_job` finds the inbox key already present (DUPLICATE) and acks without running;
  3. the crashed worker's heartbeat stops;
  4. after `WORKER_HEARTBEAT_TTL_SECONDS` (30), the leader's `recover_jobs` fails the attempt as `lost` (provided its observation window already spans a TTL; otherwise one TTL later);
  5. a **new** dispatch (attempt N+1) goes through the outbox.

  If the "crashed" worker comes back and reports late, `_live_attempt` rejects it. This is covered by `test_dead_worker_jobs_are_recovered_and_stale_worker_is_fenced` and the production drill `test_killed_workers_are_detected_and_their_jobs_retried`.

**Q: How do you prevent duplicate processing?**

Three layers:
1. `Idempotency-Key` with a partial unique index on `(tenant_id, idempotency_key)`, against duplicate submissions;
2. the inbox (`inbox_records`, key `job-executor:<message_id>`), against duplicate deliveries;
3. attempt + worker fencing, against stale messages and zombie workers.

**Q: What's the transactional outbox and why do you need it?**

Writing the job and publishing the message are two systems: a crash between them loses the message (write first) or creates a ghost (publish first). The outbox makes the message a row in the same DB transaction as the job. A relay publishes it later, one transaction per row (`relay_outbox_once`):
- `FOR UPDATE SKIP LOCKED`, so concurrent relays never take the same row;
- publish with confirms;
- mark it dispatched.

If the relay crashes after the broker confirms but before it commits, it republishes; the inbox absorbs that duplicate.

**Q: How do you make sure only one scheduler acts?**
- **The lease:** `PostgresLeaderElection` (`infrastructure/coordination/leader_election.py`) uses one atomic `INSERT … ON CONFLICT DO UPDATE … WHERE (holder = me OR expired)`. Each new term increments a **fencing token**.
- **Fencing:** before writing, the leader re-checks `holds_lease(token)` with `SELECT … FOR SHARE` *inside the same transaction*. A leader paused by GC or a network partition, whose lease expired, can't commit. Tested in `test_superseded_scheduler_is_fenced_out`.

**Q: Clocks?**

Leases, heartbeat ages, backoff and dead-worker verdicts use the database clock (`store.now()` / `now()` in SQL), so process clock skew doesn't matter. The one local clock is the worker's self-fencing deadline: a monotonic clock measured from the *start* of its last successful heartbeat. The database stamps the heartbeat later than that start, so the worker's deadline always falls before the scheduler's verdict. It needs the clocks to tick at the same rate, not to agree; the margin (10 s with defaults) absorbs drift and cancellation latency.

**Q: What happens to running jobs during a PostgreSQL outage?**
- **Under 20 s:** nothing visible. Claims fail and are requeued; a finished job's report retries until it commits (C2 fix, `JobExecutor._report`). No job re-runs.
- **Over 20 s:** every worker fences: it cancels its handlers, stops consuming, and hands the attempts back (`lost`, retried) once PostgreSQL returns. The scheduler, after reconnecting, waits a full TTL of observation before declaring anyone dead, so workers that come back keep their registration.
- **Why stop at all if everyone lost the database?** A worker can't tell a global outage from being partitioned alone. If it's alone, the scheduler *will* retry its job at 30 s. Stopping costs a retry; not stopping risks two concurrent copies.

**Q: The original bug (C1): how did you find and prove it?**

Audit by reading the recovery path, then a reproduction before any fix: an in-process TCP proxy between PostgreSQL and the workers/scheduler, so an outage can be simulated without stopping a container; a wrapped handler records each run's start and end. Scheduler reconnecting first, production timings: attempt 2 started at 46.5 s while attempt 1 ran until 150.4 s, 104 s of overlap. After fix 1 (observation window) that scenario passed; a worker-only partition still overlapped by 24 s until fix 2 (self-fencing). Both drills are in the repo (`test_c1_postgres_outage_overlap.py`, `test_worker_self_fencing_drills.py`).

**Q: Why an observation window rather than a longer TTL?**

A longer TTL only moves the problem: an outage longer than the new TTL produces the same false verdicts, and real crashes are detected later every time. The window costs nothing in steady state and only delays the sweep after an interruption, by exactly one TTL. Lease expiry and everything else keep running.

**Q: Why self-fencing on the worker instead of renewing a per-job lease?**

Lease renewal is a database write, so it fails in exactly the situation we care about (the worker can't reach PostgreSQL); the worker still wouldn't know whether its lease was taken. What matters is that the worker stops *before* anyone may take over, which it can compute locally from its last successful heartbeat. Handlers are already bounded by their timeout, and the job lease (timeout + grace) still backs that up.

**Q: Why sticky epochs?**

Because "a heartbeat succeeded again" says nothing about the gap before it. If a worker was silent for 40 s, the scheduler may already have retried its job. Fencing bumps the epoch; attempts from the old epoch can never report success, only be handed back, while new work after recovery runs under the new epoch.

**Q: A handler finished in time, but its result can only be written after the worker's authority lapsed. Accept it?**

Yes (design "option A"): authority is required while *executing*; recording is a compare-and-set on the attempt. If the scheduler already recovered the attempt, the report is rejected; if not, nobody else acted on it and recording it avoids a needless re-run. Rejecting late-but-valid results with an extra lease check would *add* duplicate side effects. A handler that finished after the fence is never reported, only handed back.

**Q: What if a handler ignores cancellation or blocks the event loop?**

Cancellation in asyncio is cooperative, and a blocked loop also stops the heartbeat and the fencing watchdog. So a separate thread (`infrastructure/coordination/fail_stop.py`) exits the process (code 70) if any handler is still alive TTL − interval (25 s) after the last good heartbeat. Every attempt in the process has lost authority by then, so exiting kills nothing that was still allowed to run; Docker restarts it and the scheduler retries. Tests inject the exit function; it is never armed when tests construct a worker directly.

**Q: What was C2?**

After a handler finished, the worker tried to record the result 3 times over about 1.5 s, then gave up and acked the message. The job sat RUNNING until its lease expired, and the scheduler re-ran it. Now `_report` retries until the report commits, is rejected, or the attempt's lease deadline passes; a commit with a lost reply is read back. Reproduced first (2.5 s outage → handler ran twice), then fixed (→ ran once).

**Q: Any other edge you found while doing this?**

Two:
- **Stranded claims.** If the claim's COMMIT succeeds but the reply is lost, the worker requeues; the redelivery is an inbox duplicate, and the job would sit RUNNING with no handler until its lease expired. Now the worker sees the job RUNNING under itself with nothing running and hands it back.
- **Draining without heartbeats.** My first self-fencing version stopped heartbeating when shutdown began, so a job finishing late in a 25 s drain would lose authority and be re-run. A real container restart in testing showed it; I added a failing test, then kept heartbeating until the drain ends.

## RabbitMQ

**Q: Why RabbitMQ instead of Redis queues?**

RabbitMQ gives, out of the box, exactly what job delivery needs. NEXUS uses each of these:
- per-message **acknowledgements**, with **redelivery** when a consumer's connection drops;
- **publisher confirms**, so the outbox knows the broker has the message;
- **mandatory** routing, so an unroutable publish is an error rather than silently dropped;
- **priority queues** (`x-max-priority: 10`);
- **dead-letter exchanges** (`nexus.dlx`);
- **prefetch** back-pressure (`prefetch_count = capacity`).

Redis can approximate this (Streams with consumer groups and `XPENDING`/`XCLAIM`), but you'd hand-build redelivery, dead-lettering and confirms. Redis is better at what NEXUS uses it for: cheap shared counters. The trade-off is one more piece of infrastructure to run.

**Q: Describe the topology.** (`infrastructure/messaging/rabbitmq_topology.py`)
- Exchange `nexus.events` (topic):
  - `job.dispatch_requested` → queue `nexus.jobs` (durable, priority 0–10, DLX `nexus.dlx`);
  - `task.dispatch_requested` → `nexus.tasks`.
- Exchange `nexus.dlx` → `nexus.dead-letter`.
- Everyone declares this idempotently on connect.

**Q: Ack, nack, reject: when?** (`rabbitmq_worker.py` `_on_job_message`)
- **ack** after the result is recorded, including a failure that scheduled a retry, because the retry is a new message;
- **reject without requeue** (→ DLQ) for a final failure or an unparseable message;
- **nack with requeue** (after a 1-second pause) when the claim itself failed, for example the DB was briefly down.

**Q: What happens if RabbitMQ is unavailable?**
- The API still accepts jobs. They're committed with their outbox rows and stay **PENDING**.
- The relay's publish fails, so `record_failure` backs off the row (5 s, doubling, up to 1 h).
- `/api/v1/ready` returns 503.
- When RabbitMQ is back, the relay publishes and the jobs run.
- Workers reconnect automatically (`connect_robust`).

This is drilled in CI: `test_jobs_submitted_while_the_broker_is_down_run_when_it_returns`.

**Q: How are retries delayed without a delay plugin?**

The retry's outbox row gets `available_at = next_attempt_at`, and the relay only publishes due rows. So the delay lives in PostgreSQL and survives broker restarts (`outbox_repository.py`, `HOLD_UNTIL_KEY`).

## Database

**Q: How is job state stored?**
- `jobs`: the current state, plus a `version` column;
- `job_transitions`: append-only history;
- `job_executions`: one row per attempt.

Transitions come from the pure state machine in `domain/job.py` and are persisted in the same transaction as the state change.

**Q: How do you avoid race conditions between the relay, workers and the scheduler?**
- Row locks: `get_for_update` → `SELECT … FOR UPDATE`.
- The dispatch hook **locks the job before publishing**. A worker that gets the message instantly blocks on the row until the relay's transaction records QUEUED, so it never sees PENDING.
- Recovery queries use `FOR UPDATE SKIP LOCKED` batches.

**Q: What happens if the database is unavailable?**
- The API fails requests (`/ready` 503).
- Workers can't claim, so they nack messages back to RabbitMQ (nothing is lost; messages wait in the queue).
- The scheduler's passes fail, and its health check turns unhealthy.

Everything resumes when PostgreSQL returns, as the PostgreSQL-restart drill shows. During the outage, jobs can't be submitted; NEXUS doesn't buffer submissions outside the DB.

**Q: Migrations?**
- Alembic, in `migrations/versions/`.
- In Docker, a one-shot `migrate` service runs `alembic upgrade head`, and every app service waits for it to finish successfully.
- CI checks `alembic check` (models match migrations) and a full downgrade.

**Q: Indexes worth mentioning?**
- The partial unique index `uq_jobs_tenant_idempotency_key` (`WHERE idempotency_key IS NOT NULL`);
- the unique `(job_id, attempt)` on `job_executions`;
- `ix_job_executions_started`, for history queries.

## Redis

**Q: What is Redis used for?**

Only per-tenant rate limiting (`api/rate_limit.py`): `INCR` on `rl:<scope>:<tenant>:<window>` with a 65-second expiry, giving fixed one-minute windows.

**Q: Why fixed windows, and what if Redis is down?**
- **Fixed windows:** simple, at the cost of allowing up to 2× the limit around a window boundary. 🔮 A sliding-window log or token bucket would smooth that.
- **Redis down:** the code catches the error and falls back to an in-process counter, so limits become per API process. The API keeps serving. Redis holds no job state.

## Docker

**Q: How do services find each other?**

On the Compose network, by service name (`postgres`, `rabbitmq`, `api`). Host ports are published only on `127.0.0.1`.

**Q: How do you guarantee startup order?**

`depends_on` with `condition: service_healthy` and `service_completed_successfully`:
1. infrastructure becomes healthy;
2. `migrate` completes;
3. the API, scheduler and workers start;
4. the dashboard and monitoring start.

Health checks are real: the API's is `/api/v1/ready`, which checks all dependencies; the daemons serve `/health` on 8081.

**Q: Graceful shutdown?**

SIGTERM → the worker marks itself draining, stops consuming, gives in-flight jobs `WORKER_SHUTDOWN_GRACE_SECONDS` (25), hands the rest back with `abandon` (→ RETRYING), and deregisters. `stop_grace_period: 40s` is longer than that grace, so Docker doesn't SIGKILL first.

**Q: Production differences?** (`compose.production.yml`)
- every secret is required;
- only Caddy is public, with automatic HTTPS;
- memory limits and `no-new-privileges`;
- non-root processes;
- log rotation;
- worker replicas.

## CI/CD

**Q: What runs in CI?**

`.github/workflows/ci.yml`:
- lint, types and unit tests;
- integration tests with real service containers;
- frontend tests and build;
- the full stack via Compose with e2e tests and a demo seed;
- the production stack on official images with destructive failure drills.

`pages.yml` builds the static demo. Details are in LEARNING_GUIDE §19.

**Q: How do you test failure handling, not just happy paths?**

`tests/e2e/test_production_resilience.py` restarts, stops and SIGKILLs real containers while real jobs run:
- the API, workers, PostgreSQL, Redis and RabbitMQ;
- a broker outage;
- a full stop and start.

It then asserts that every job finished with **exactly one completed execution** recorded. That is about state, not handler runs: an attempt interrupted by a crash is recorded `lost` and its handler may have run partly or fully. It runs in CI against the official images.

## Reliability and failure scenarios

**Q: How do retries work?**
- `Job.fail()` (`domain/job.py`): if attempts remain and the error isn't permanent, the job goes FAILED → RETRYING with `attempt + 1` and `next_attempt_at = now + backoff`.
- **Backoff:** `retry_backoff` = `initial × 2^(attempt−1)`, capped at `max_backoff`.
- `_fail_running` writes a held-back outbox message.
- `PermanentTaskError` (or a result that isn't JSON-serializable) skips retries.
- When attempts run out, the job stays FAILED with `dead_lettered_at`, and the message goes to `nexus.dead-letter`.
- A manual or bulk retry gives a fresh attempt budget.

**Q: What's in the dead-letter queue, and how do you recover?**
- Final failures and poison messages.
- Operators filter dead-lettered jobs by error in the dashboard or API, then retry them individually or in bulk.
- The leader prunes broker DLQ messages whose jobs were since retried (`messaging/dead_letter.py`). It only deletes what it can attribute to a resolved job, and requeues everything else.

**Q: A job hangs forever. Then what?**

`asyncio.wait_for` enforces `timeout_seconds`. If the handler ignores cancellation or blocks the event loop, heartbeats stop and the fail-stop thread exits the worker 25 s after the last good heartbeat. As a final backstop, the job's **execution lease** (`timeout + TASK_LEASE_GRACE_SECONDS`) expires, and the scheduler fails it (`timed_out`) and retries.

**Q: What if the scheduler dies?**

Jobs keep running: the workers and the API's eager relay don't need it. But retries wait (their dispatch sits in the outbox), and recovery pauses. A second scheduler replica would take over within the 15-second lease. Docker restarts the crashed process (`restart: unless-stopped`). A new leader starts with an empty observation window, so it declares no worker dead during its first TTL: dead-worker recovery is one TTL slower after a failover, by design.

## Scaling and design trade-offs

**Q: How do you scale workers?**
- Add processes: `docker compose up -d --scale worker=4`, or `WORKER_REPLICAS` in production. Prometheus discovers them through DNS.
- Each worker's concurrency is `WORKER_CAPACITY`, enforced by RabbitMQ `prefetch`.
- Workers are stateless, so scaling is just more consumers on `nexus.jobs`.

**The limits:**
- PostgreSQL write throughput, since every transition is a transaction;
- one queue for all job types;
- connection pools: each worker holds DB connections, sized by `DB_POOL_SIZE`.

**Q: How would you implement fair scheduling?**

Today:
- **Priority:** jobs carry 4 priorities mapped to RabbitMQ priorities, so `critical` overtakes `low`.
- **Rate limiting at submission** is per tenant.
- **There is no fairness between tenants at execution time:** one tenant can fill the queue.

🔮 Designs to discuss:
1. **Per-tenant queues with weighted round-robin consumption:** workers consume from many queues, which is simple but means many queues.
2. **Pull-based fair claim from PostgreSQL:** deficit round-robin over tenants, `SELECT … FOR UPDATE SKIP LOCKED` ordered by each tenant's virtual time, with RabbitMQ used only for wake-ups.
3. **Per-tenant concurrency caps** checked in `claim_job`: over the cap → requeue with a delay.

The trade-off is throughput and simplicity against isolation. Option 3 is the smallest change to the current code. See ROADMAP phase 2.

**Q: How would you implement dependency-based workflows (DAGs)?**

Partly **implemented**, in the separate **tasks** subsystem (`/api/v1/tasks`, ADR 0004):
- tasks accept `depends_on` and `not_before`;
- the scheduler's reconciler (`use_cases/reconcile_tasks.py`, `domain/task_readiness.py`) promotes a PENDING task when all its dependencies have COMPLETED;
- it cancels a task, and its dependents, when a dependency can no longer succeed;
- tasks run on the same RabbitMQ workers (`nexus.tasks` queue).

The **jobs** API has no dependencies. 🔮 Unifying them would mean:
- a `workflow` entity;
- edges stored in PostgreSQL;
- promotion on each `report_success` instead of polling;
- cycle detection at submission;
- fan-in/fan-out tests.

See ROADMAP phase 3.

**Q: What are the limitations of the current design?**
- **At-least-once handler execution:** side effects can repeat (one after another, never concurrently).
- **H-1, interrupted attempts consume retries:** shutdown, crash recovery, self-fencing hand-backs and lease expiry each count against `max_attempts`, so a `max_attempts=1` job can be dead-lettered by a deploy or a long PostgreSQL outage. Documented in ROADMAP; the likely fix is to stop counting `lost` attempts, or cap them separately.
- **Outages over 20 s interrupt running jobs:** the price of never running two copies at once.
- **Coverage gaps:** self-fencing and report retry apply to the RabbitMQ worker daemon only. The Vercel job path relies on lease expiry alone (Functions don't heartbeat), the tasks DAG pipeline (`TaskExecutor`) is unchanged, and the stateless cron maintenance endpoint has no observation window.
- **One PostgreSQL:** it's the bottleneck and the single point of failure. Every state change is a write.
- **One job queue:** no fair scheduling across tenants and no per-type isolation (a slow type shares capacity with fast ones).
- **Polling:** the relay polls every second, and redispatch of lost messages waits 15 minutes. 🔮 `LISTEN/NOTIFY` could make relaying event-driven.
- **Rate limiting:** fixed-window and per tenant only; no per-key roles.
- **Retention:** no history retention policy; `job_executions` grows forever, and the metrics queries scan it.
- **Limited handlers:** only built-in types (`echo`, `http.webhook`, test types). No user code runs, which is by design for safety, but it limits what jobs can do.
- **Production setup:** a single server, with no off-site backups (`docs/FINAL_DEPLOYMENT_REVIEW.md`).

**Q: What would you do differently next time?**

Honest options:
- start with the jobs model only (the tasks/DAG subsystem came first and now duplicates concepts);
- design retention and partitioning of `job_transitions` and `job_executions` up front;
- add a load test before tuning anything.

## Quick numbers to remember

| Setting | Default | File |
|---|---|---|
| Heartbeat interval / TTL | 5 s / 30 s | `infrastructure/config.py` |
| Worker self-fences / fail-stop | 20 s / 25 s after the last good heartbeat | `worker_self_fence_after_seconds`, `worker_fail_stop` |
| Observation window before dead-worker sweeps | 1 TTL (30 s) of uninterrupted leadership | `observation_window.py` |
| Report retry backoff | 0.5 s doubling to 5 s, until the lease deadline | `job_executor.py` |
| Leader lease | 15 s | `scheduler_lease_duration_seconds` |
| Maintenance tick / relay poll | 5 s / 1 s | `daemon_tick_interval_seconds`, `outbox_relay_poll_interval_seconds` |
| Worker shutdown grace | 25 s | `worker_shutdown_grace_seconds` |
| Lost-dispatch redispatch | 900 s | `job_redispatch_after_seconds` |
| Default timeout / attempts | 60 s / 3 | `job_default_*` |
| Relay publish backoff | 5 s doubling to 1 h | `outbox_relay_*_backoff_seconds` |
| Rate limits | 600 reads, 120 writes per minute per tenant | `rate_limit_*` |
