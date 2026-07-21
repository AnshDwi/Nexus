# ADR 0001: Exactly-Once Effect Application via the Inbox Pattern

## Status

Accepted (implemented in Milestone 1).

## Context

Nexus consumes events from RabbitMQ, which guarantees at-least-once delivery
under redelivery/retry. Any consumer that applies an effect (writes state,
charges a balance, records a job run) directly from a message handler is at
risk of applying that effect more than once if the message is redelivered —
after a consumer crash between processing and acknowledging, for instance.

True end-to-end exactly-once delivery is not achievable over an unreliable
network (see ADR 0002's engineering discussion for the full argument). The
question is not "how do we get exactly-once delivery" but "how do we make
redelivery safe."

## Decision

Every consumer that applies a side effect must record having done so in an
`inbox_records` table, keyed by the event's idempotency key, in the **same**
Postgres transaction as the side effect itself. The insert uses
`INSERT ... ON CONFLICT DO NOTHING RETURNING`, which is atomic and race-safe
under concurrent consumers competing for the same message.

If the insert reports the key as already present, the handler is skipped —
the effect was already applied by an earlier delivery.

## Consequences

- Effects are applied exactly once, provided every consumer honors this
  pattern. A consumer that skips it only gets at-least-once, and can
  double-apply effects.
- Correctness was verified under real concurrency, not just asserted: an
  integration test fires 10 concurrent claim attempts at the same
  idempotency key against a live Postgres instance and asserts exactly one
  succeeds.
- This is the consumer-side half of the Outbox + Idempotent Consumer pattern
  documented fully in ADR 0002.
