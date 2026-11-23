# ADR 0004: Vercel-first task execution

## Status

Accepted. Builds on ADR 0001 (inbox), 0002 (outbox) and 0003 (leader lease).

## Context

Vercel is the deployment target. Vercel runs request-driven Functions (Fluid
compute), managed Queues (public beta), Cron Jobs and, in public beta,
Daemons (long-running container processes, production deployments only).
It does not run Docker Compose services, RabbitMQ or any other broker, and an
ordinary request function must not host an infinite loop.

NEXUS had correct kernels (outbox, inbox, fencing-token lease, task state
machine, admission controller) but no runtime: tasks could not be stored,
dispatched, executed or reported.

## Decision

1. **The FastAPI app is one Vercel Function.** `[tool.vercel] entrypoint =
   "vercel_app.api:app"`; `vercel_app/` is a path shim for the `src/` layout,
   with no logic. The lifespan builds a small per-instance SQLAlchemy pool,
   which is safe on Fluid compute.
2. **Tasks are persisted** (`tasks`, `task_transitions`). Every state change
   goes through the existing `TaskInstance.transition_to` table. Submission
   writes the task and its dispatch request (outbox row) in one transaction.
3. **Vercel Queues carries dispatch.** The outbox relay publishes
   `task.dispatch_requested` events to the topic `nexus-task-dispatch`, using
   the outbox `event_id` as the queue idempotency key, through a new
   `VercelQueuesEventPublisher` behind the existing `EventPublisher` port.
   RabbitMQ stays available via `EVENT_BUS=rabbitmq` for self-hosting.
4. **Execution runs in the queue's push consumer.** That consumer is a
   private, queue-triggered function generated from `[[tool.vercel.subscribers]]`.
   It claims the task (READY→DISPATCHED→RUNNING under a row lock, only for the
   attempt number in the message), runs the registered handler with a
   timeout, and records the result guarded by attempt number. The attempt
   number is the per-task fencing token.
5. **Recovery is layered:**
   1. *Eager relay*: the request or worker that commits an outbox row
      publishes it before returning.
   2. *Queue redelivery*: an unacknowledged message is retried. A delivery
      that arrives while another worker's lease is live is deferred with
      `RetryAfter`.
   3. *Reconciler*: fails attempts whose lease expired, re-dispatches stale
      READY tasks, promotes due PENDING tasks, cancels tasks blocked by failed
      dependencies, and drains the outbox. It runs from Vercel Cron
      (`/api/v1/internal/maintenance`, `CRON_SECRET`) and, optionally, from the
      `scheduler` Daemon. Both serialize through the existing
      `PostgresLeaderElection` lease, which now uses database time.
6. **Daemons are optional.** They do not run in preview deployments, so no
   guarantee may depend on them. They shorten recovery latency from "next
   cron" to seconds. A poll-mode `worker` daemon role exists for tasks longer
   than a Function's `maxDuration`.

## Consequences

**Guarantees:**
- A committed task is never lost. Its dispatch request is in the same
  transaction, and is retried by relay backoff, the cron sweep, queue
  redelivery and the reconciler.
- An attempt runs on at most one worker at a time. A stale worker's result is
  discarded.
- Execution is at-least-once. Handlers get a stable idempotency key.
- Retries follow the task's `RetryPolicy` (exponential backoff, capped).
  `PermanentTaskError` and exhausted attempts end in PERMANENTLY_FAILED, and
  dependents are cancelled transitively.

**Costs and limits:**
- Task duration is bounded by the consumer function's `maxDuration`.
  `TASK_MAX_TIMEOUT_SECONDS` (240) plus `TASK_LEASE_GRACE_SECONDS` (30) stays
  under the 300s default.
- Messages are pinned to the deployment that published them, so old
  deployments keep consuming their own messages after a promote. Migrations
  must stay backward compatible (expand/contract).
- Without daemons, recovery from the rare failure modes waits for the next
  cron: once a day on Hobby, once a minute on Pro.
- Prometheus counters are per Function instance. The tables are the source of
  truth for outcomes.
