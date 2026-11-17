# ADR 0005: Job pipeline over RabbitMQ with confirmed dispatch and fenced recovery

## Status

Accepted. Builds on ADRs 0001–0004.

## Context

The product needs a user-facing **Job** with an explicit lifecycle:

    PENDING → QUEUED → RUNNING → COMPLETED
    RUNNING → FAILED → RETRYING → QUEUED

It also needs a dashboard, and messaging that stays on RabbitMQ. ADR 0004
added Vercel Queues as a transport for the DAG task pipeline. That remains
available, but it must not silently replace RabbitMQ.

## Decision

1. **A separate `Job` aggregate** (`domain/job.py`) with its own transition
   table. The DAG `Task` state machine (ADR 0004) is unchanged, so neither
   model is distorted to fit the other. Shared building blocks are reused:
   - `retry_backoff`
   - the handler registry
   - the admission controller (retry-policy validation)
   - the outbox, inbox, worker registry and leader lease.
2. **Dispatch confirmation.** `relay_outbox_once` gained an optional
   `OutboxRelayHook`, which is backward compatible. `JobDispatchHook` does two
   things:
   - it locks the job row *before* publishing and skips obsolete attempts;
   - it writes PENDING/RETRYING → QUEUED in the same transaction, only after
     `publish()` returned.

   The RabbitMQ publisher uses publisher confirms. Work messages are
   `mandatory` with returns raised, so an unroutable message is an error,
   never a silent drop.
3. **Retry backoff through the outbox.** The outbox writer honours
   `metadata.deliver_after` by setting `available_at`, so the relay holds a
   retry dispatch back until it is due. No broker-side delay plugin is
   needed.
4. **Topology.**
   - `nexus.events` (topic exchange) routes `job.dispatch_requested` to
     `nexus.jobs`. That is a durable classic queue with `x-max-priority=10`,
     so job priority maps to AMQP priority.
   - `task.dispatch_requested` routes to `nexus.tasks`.
   - Both queues dead-letter to `nexus.dlx` → `nexus.dead-letter`.
5. **Workers** (`RabbitMQJobWorker`):
   - register in `worker_registrations` and heartbeat on the database clock;
   - consume with `prefetch = capacity`;
   - claim jobs through the inbox plus an attempt check;
   - execute with a timeout;
   - report under the `attempt` + `worker_id` fence;
   - ack after commit;
   - reject to the DLQ on a poison message or a final failure;
   - drain gracefully on SIGTERM.
6. **Scheduler recovery under fencing.** Each pass first verifies the lease's
   fencing token with `SELECT … FOR SHARE`, which blocks a takeover until the
   pass commits. It then:
   - fails and retries RUNNING jobs whose worker missed its heartbeat TTL;
   - fails and retries jobs whose execution lease expired;
   - re-dispatches dispatchable jobs whose message was lost;
   - forgets long-dead workers.

   A replica that lost the lease aborts the pass.
7. **Dashboard.**
   - React + Vite + Tailwind in `frontend/`.
   - Talks only to the public API, authenticated with a tenant API key that
     stays in the browser.
   - Refreshes every 3 seconds.
   - Deploys as its own Vercel project.

## Consequences

- **Delivery semantics:**
  - A job is never shown QUEUED unless the broker holds its message.
  - A committed job is never lost: outbox retries, re-dispatch and recovery
    cover it.
  - Execution is at-least-once, and handlers receive a stable idempotency
    key.
- **Two pipelines.** Jobs (simple, priority, manual retry) and Tasks (DAG,
  `not_before`). Both share the outbox, relay, worker process and
  operational tooling.
- **Hosting.** On Vercel, workers and the scheduler need Daemons (beta,
  production only), and RabbitMQ is an external managed service. Previews can
  submit and inspect jobs, but nothing executes them unless a worker is
  connected to the same broker and database.
