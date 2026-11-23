# NEXUS roadmap

Learning-focused engineering phases. Each one deepens a concept you can explain in an interview, and each has a concrete definition of done. They're ordered by value for learning; phase 1 is the recommended next step.

## Done: execution-safety fixes (reliability remediation)

Found by an audit, reproduced with failing drills first, then fixed (see README → *Reliability* and LEARNING_GUIDE §21):
- **C1 / fix 1, scheduler observation window:** no dead-worker verdicts until the leader has watched heartbeats for a full TTL (`infrastructure/coordination/observation_window.py`).
- **C1 / fix 2, worker self-fencing:** execution authority with sticky epochs, handler cancellation, hand-back, stranded-claim recovery, `TaskContext.check_authority()`, fail-stop watchdog (`execution_authority.py`, `fail_stop.py`, `rabbitmq_worker.py`, `job_executor.py`).
- **C2, report retry:** a finished result is recorded through database blips instead of being re-run (`JobExecutor._report`).

Not covered by these fixes (candidate follow-ups):
- **Vercel job path** (`EVENT_BUS=vercel_queues`): Functions don't heartbeat, so there is no self-fencing; only lease expiry protects it.
- **Tasks DAG pipeline** (`TaskExecutor`): attempt-guarded, but no self-fencing and no report retry.
- **Cron maintenance endpoint** (`/internal/maintenance`): stateless, so no observation window; it sweeps dead workers immediately. A database-based window (for example, the leader's term start) would cover it.

Ground rules for every phase:
- **Write the failing test first.**
- **Measure before and after** where performance is involved.
- **Update LEARNING_GUIDE.md and INTERVIEW_GUIDE.md** when behaviour changes.
- **Keep `docker compose up --build -d` working.**

---

## Outstanding issue H-1 — infrastructure failures consume the retry budget

Not part of any phase yet; recorded so it is not forgotten.

Every way an attempt can end without the handler itself failing is recorded as a failed attempt and counts against `max_attempts`:
- graceful shutdown (`worker shut down before completion`);
- dead-worker recovery by the scheduler (`stopped heartbeating`);
- worker self-fencing hand-backs (`self-fenced`) and stranded-claim hand-backs;
- lease expiry.

So a job with `max_attempts=1` that happens to be running during a deploy, a PostgreSQL outage longer than the self-fencing deadline (20 s by default), or a worker crash is dead-lettered although its handler never failed. All of these go through `Job.fail` (`domain/job.py`); the execution row's status (`lost`, `timed_out`) already tells them apart.

Direction (not decided): do not count `lost` attempts against `max_attempts`, or count them in a separate, capped counter so a job that keeps killing workers still stops. Keep real timeouts counting. Needs a migration only if a separate counter is stored.

---

## Phase 1 — Reliability: idempotent effects and data lifecycle

**Objective.** Close the two gaps that the current design admits:
- handler side effects can repeat one after another (at-least-once execution; concurrent runs are already prevented by self-fencing);
- history tables grow forever.

**Why it matters.** "Can a job run twice?" is the first hard interview question. Today the honest answer is "state changes once; two attempts never run at the same time; but a handler can run again after a crash". This phase shows the standard way receivers make that harmless. Unbounded tables are the most common way a working system decays.

**Concepts learned:**
- idempotency keys for downstream calls;
- the idempotent-receiver pattern;
- data retention;
- table partitioning;
- `VACUUM`/bloat;
- an at-least-once vs exactly-once vocabulary.

**Already in place:** handlers receive a stable key, `TaskContext.idempotency_key` (the job ID, the same for every attempt), and `http.webhook` sends it as `Idempotency-Key`. `JOB_REDISPATCH_AFTER_SECONDS` is configurable, and the `NexusOutboxLag` alert exists.

**Features:**
- An example idempotent receiver (a small test service, or a `processed_keys` table for an internal handler) that drops repeats of the same key, to demonstrate the pattern end to end.
- A retention job in the scheduler pass deletes `job_executions`, `job_transitions`, `inbox_records`, dispatched `outbox_records` (and completed jobs) older than `RETENTION_DAYS`, in batches.
- Optional: partition `job_transitions` by month (Alembic migration).

**Tests:**
- An integration test where a handler calls the example receiver, the worker is killed after the side effect but before the report, and the receiver records the effect once although the handler ran twice.
- Retention: insert old rows, run a pass, assert they're deleted, recent ones kept, and nothing deleted for running jobs.

**Definition of done:**
- Killing a worker mid-webhook doesn't double-apply at a receiver that honours the key, and this is verified by a test.
- With retention enabled, a 30-day-old history is pruned, and the metrics query time stays flat on a seeded 1M-row table (documented measurement).

## Phase 2 — Fair scheduling

**Objective.** Stop one tenant from monopolizing workers.

**Why it matters.** It's a classic multi-tenant systems problem, and a strong discussion topic (see INTERVIEW_GUIDE "fair scheduling").

**Concepts learned:**
- per-tenant concurrency limits;
- weighted fair queuing;
- deficit round-robin;
- starvation;
- back-pressure;
- throughput/fairness trade-offs.

**Features (incremental):**
1. **Per-tenant concurrency cap.** `claim_job` counts the tenant's RUNNING jobs. Over `TENANT_MAX_RUNNING`, it returns a new `DEFERRED` decision; the worker nacks with a delay (or re-dispatches through the outbox with `available_at` set a few seconds ahead).
2. **Weights:** an optional per-tenant weight in `API_KEYS` config.
3. **Visibility:** a "waiting because of the tenant cap" status in the queue view, and a metric `nexus_jobs_deferred_total{tenant}`.
4. 🔬 Stretch: a pull-based fair claim from PostgreSQL (deficit round-robin), with RabbitMQ used as a wake-up signal only.

**Tests:**
- Unit: the cap decision.
- Integration: tenant A submits 50 jobs, tenant B 5, with 2 workers × capacity 2. B's jobs start within N seconds, A never exceeds its cap, and every job finishes with one completed execution.

**Definition of done:**
- B's 95th-percentile wait time under A's flood is bounded and measured;
- no starvation in a 10-minute soak;
- documented in the learning guide.

## Phase 3 — Workflow orchestration (DAG) for jobs

**Objective.** Bring dependencies to the **jobs** API, reusing ideas from the existing tasks subsystem (`depends_on`, `domain/task_readiness.py`, `use_cases/reconcile_tasks.py`).

**Why it matters.** Pipelines (build → test → deploy) are the most common real use of job systems. It exercises graph algorithms and consistency.

**Concepts learned:**
- DAGs;
- topological order;
- cycle detection;
- fan-out/fan-in;
- failure propagation policies;
- event-driven vs polling promotion.

**Features:**
- `POST /api/v1/workflows` with jobs and edges, validated as acyclic at submission (Kahn's algorithm).
- Jobs with unmet dependencies wait in a new `BLOCKED` state, or reuse PENDING without a dispatch.
- On `report_success`, promote ready dependents **in the same transaction** (event-driven, no polling).
- Failure policy per workflow: `cancel_dependents` (the default) or `continue`.
- Dashboard: a workflow graph view.

**Tests:**
- Unit: cycle detection; promotion order.
- Integration: a diamond (A → B, C → D) where D runs only after both; a failure cancels the dependents; a crash during promotion doesn't lose or double-promote (fenced like the rest).

**Definition of done:** a diamond and a 50-node random DAG both complete in topological order under the resilience drills (worker kill mid-workflow).

## Phase 4 — Observability

**Objective.** Go from "metrics exist" to "you can answer why a job was slow".

**Why it matters.** Debugging distributed systems needs tracing; it's also a standard interview area.

**Concepts learned:**
- distributed tracing (OpenTelemetry);
- context propagation through a broker;
- RED/USE metrics;
- SLOs and burn-rate alerts;
- histograms vs summaries.

**Features:**
- OpenTelemetry spans: API request → outbox → relay publish → worker claim → handler → report. The trace context travels in the AMQP message headers.
- Jaeger or Grafana Tempo in the `observability` compose profile.
- Histograms for queue wait time (`queued_at` → `started_at`) and run time.
- SLOs: "95% of `normal` jobs start within 10 s"; burn-rate alerts in `alerts.yml`, with promtool tests.
- Alertmanager in compose, with a local webhook receiver, so alerts are visible without paid services.

**Tests:**
- An integration test asserting that one trace contains the API, relay and worker spans with the same trace ID.
- promtool tests for the new alerts.

**Definition of done:** from a slow job in the dashboard, one click shows its trace, and the waterfall explains where the time went.

## Phase 5 — Performance testing

**Objective.** Know the system's limits with numbers.

**Why it matters.** "How many jobs per second can it do, and what breaks first?" Today the answer is "not measured".

**Concepts learned:**
- load generation;
- throughput vs latency;
- Little's law;
- connection-pool sizing;
- profiling async Python;
- PostgreSQL `EXPLAIN ANALYZE`;
- RabbitMQ prefetch tuning.

**Features:**
- A load script (Locust or a small asyncio client) that submits jobs at a set rate and measures submit latency and end-to-end completion.
- A results doc: throughput at 1, 2, 4 and 8 workers; p50/p95/p99 queue wait; CPU, memory and DB connections; the first bottleneck found.
- One targeted optimization from the findings (for example, batching relay publishes, or `LISTEN/NOTIFY` to wake the relay instead of polling), with before/after numbers.

**Tests:** the load script runs in CI at a small rate as a smoke test; the full runs are local and documented.

**Definition of done:** a "capacity" section in the README with measured numbers and the identified bottleneck; one improvement, quantified.

## Phase 6 — Security

**Objective.** Move from "tenant API keys" to defensible access control.

**Why it matters.** Authorization and secret handling are expected knowledge for backend roles.

**Concepts learned:**
- RBAC;
- key rotation;
- hashing and peppering;
- audit logs;
- SSRF protection;
- dependency and container scanning.

**Features:**
- **Key scopes:** `read`, `write`, `admin` (bulk retry and other dangerous actions require `admin`).
- **Key rotation:** multiple active keys per tenant with expiry; a CLI to issue and revoke.
- **An audit log table:** who cancelled or retried what, and when.
- **Harden `http.webhook`:** resolve DNS and block private, loopback and link-local IPs (SSRF), not just the host allowlist.
- **CI scans:** `pip-audit`, `npm audit` and a container image scan (Trivy), failing on high severity.

**Tests:** scope enforcement per endpoint; rotated keys; SSRF attempts (`http://169.254.169.254`, DNS rebinding) are rejected.

**Definition of done:** every endpoint has a documented required scope backed by a test; CI fails on a known-vulnerable dependency.

## Phase 7 — Developer experience

**Objective.** Make changes fast and safe.

**Why it matters.** Fast feedback loops are what make the other phases pleasant.

**Concepts learned:**
- hot reload in containers;
- test pyramids;
- contract testing;
- pre-commit automation.

**Features:**
- A `compose.dev.yml` override that mounts `src/` and runs the API with `--reload`, so backend edits don't need a rebuild.
- `pre-commit` hooks: ruff, format, mypy, frontend lint.
- An OpenAPI-generated TypeScript client (or a contract test), so frontend and backend types can't drift.
- A `make`-free task list in `pyproject.toml` (or `just`) only if the command list keeps growing.

**Tests:** a CI check that the generated client is up to date; pre-commit runs in CI.

**Definition of done:** editing a backend route is reflected locally in under 2 seconds without a rebuild, and type drift between API and dashboard fails CI.

---

## Suggested order

**1 → 4 → 5 → 2 → 3 → 6 → 7**:
1. reliability first, since it's the core promise;
2. tracing, because it makes everything after it debuggable;
3. load testing, to find real bottlenecks before optimizing;
4. then the bigger features.
