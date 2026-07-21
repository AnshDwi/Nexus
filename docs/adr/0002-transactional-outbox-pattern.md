# ADR 0002: Transactional Outbox Pattern

## Status

Accepted (implemented in Milestone 2). Builds on [ADR 0001](0001-exactly-once-inbox-pattern.md).

## Context

A business operation needs to both change state and notify other services of
that change. Writing to Postgres and publishing to RabbitMQ are two separate
systems with no shared transaction — there is no atomic way to guarantee
"the state changed if and only if the event was published." This is the dual
write problem. Naively publishing after commit risks publishing an event for
a transaction that later fails to commit (or the reverse: committing but the
process crashes before the publish call).

Full design rationale, failure-scenario-by-failure-scenario behavior, and the
guarantees this pattern actually provides (as opposed to what it doesn't) are
documented in [`docs/architecture/transactional-outbox.md`](../architecture/transactional-outbox.md).
This ADR records the two decisions that had genuine alternatives.

## Decision 1: Relay claiming strategy

**Chosen: `SELECT ... FOR UPDATE SKIP LOCKED`, one outbox row claimed and
resolved per transaction.**

Alternative considered: a lease-based claim/release scheme (`claimed_by` /
`claimed_until` columns), which avoids holding a Postgres row lock across the
network I/O of a RabbitMQ publish call, improving throughput at the cost of
needing lease-expiry sweeping and claim-ownership verification to avoid a
"lease expired mid-publish, two owners" race.

`SKIP LOCKED` was chosen because it satisfies every stated requirement
(restart-safe, crash-safe, safe concurrent relay replicas) with meaningfully
less machinery — one row lock is the only mechanism needed to guarantee two
replicas never process the same row concurrently. For an outbox relay
processing rows one at a time at a modest poll interval, holding a
transaction for the duration of a single publish call is an accepted,
well-established trade-off, not the throughput-sensitive hot path the
lease-based design is built for.

**Revisit this decision if:** outbox throughput becomes high enough that
lock contention or connection pool exhaustion from held transactions becomes
measurable. At that point, the lease-based design (or batched claiming) is
the documented next step.

## Decision 2: Relay deployment topology

**Chosen: the relay runs as its own process (`nexus.worker`), deployed as a
separate `worker` service in `docker-compose.yml`, sharing the API's image
but with a different entrypoint.**

Alternative considered: running the relay as an in-process background task
inside the FastAPI process. Simpler to deploy (one fewer service), but
couples the relay's lifecycle and scaling to the API's — restarting the API
would interrupt the relay and vice versa, and the two cannot be scaled
independently despite having entirely different load profiles (request
throughput vs. outbox backlog depth).

A separate process is the architecturally correct choice given the explicit
requirement that multiple relay instances run safely concurrently — that
requirement is meaningless if the relay is pinned to the API process's
lifecycle.

## Consequences

- New `worker` entrypoint and Compose service to operate and monitor
  independently of the API.
- The relay provides at-least-once delivery to RabbitMQ, not exactly-once —
  this is a deliberate, documented boundary, not a gap. Correctness depends
  on every consumer honoring ADR 0001's Inbox pattern.
- Ordering across events is not guaranteed. This is an explicit non-goal for
  this milestone, not an oversight.
