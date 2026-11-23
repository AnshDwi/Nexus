from prometheus_client import Counter, Histogram

# Leadership lease metrics.
#
# "Acquisition" and "loss" are intentionally the same metric with different
# label values, not two separate counters -- they are two views of one
# underlying event (a leadership transition), and keeping them as one metric
# avoids two counters that must always move in lockstep.
scheduler_leadership_transitions_total = Counter(
    "scheduler_leadership_transitions_total",
    "Leadership transitions observed by a replica (acquired or lost).",
    ["transition"],
)

scheduler_lease_renewals_total = Counter(
    "scheduler_lease_renewals_total",
    "Successful lease renewals by the existing holder (steady state, no transition).",
)

scheduler_lease_renewal_failures_total = Counter(
    "scheduler_lease_renewal_failures_total",
    "Errors raised while attempting to acquire or renew the leader lease "
    "(e.g. the store was unreachable) -- distinct from a clean loss to another holder.",
    ["reason"],
)

scheduler_lease_acquisition_latency_seconds = Histogram(
    "scheduler_lease_acquisition_latency_seconds",
    "Latency of a single acquire-or-renew attempt against the lease store.",
)

# Worker registry metrics.
#
# "new" vs "restarted" distinguishes a genuinely new worker_id from a
# process reusing an existing id after a restart -- both flow through the
# same register() upsert, so the label is how they stay distinguishable
# downstream.
scheduler_worker_registrations_total = Counter(
    "scheduler_worker_registrations_total",
    "Worker registrations (new worker_id, or a restart reusing an existing one).",
    ["event"],
)

scheduler_worker_heartbeats_total = Counter(
    "scheduler_worker_heartbeats_total",
    "Heartbeats received, including ones for a worker_id that isn't registered.",
    ["result"],
)

scheduler_worker_heartbeat_latency_seconds = Histogram(
    "scheduler_worker_heartbeat_latency_seconds",
    "Latency of a single heartbeat call against the registry store.",
)

# "drained" and "deregistered" are kept as one metric with two label values
# for the same reason leadership transitions are: they're both views of a
# worker's lifecycle state, not independent counters.
scheduler_worker_state_transitions_total = Counter(
    "scheduler_worker_state_transitions_total",
    "Worker lifecycle transitions (drained or deregistered).",
    ["transition"],
)

scheduler_worker_registry_failures_total = Counter(
    "scheduler_worker_registry_failures_total",
    "Errors raised by a worker registry operation (e.g. the store was unreachable).",
    ["operation", "reason"],
)

scheduler_dead_workers_detected_total = Counter(
    "scheduler_dead_workers_detected_total",
    "Workers found with a stale heartbeat by list_dead_workers.",
)

scheduler_dead_worker_sweeps_deferred_total = Counter(
    "scheduler_dead_worker_sweeps_deferred_total",
    "Maintenance passes that skipped the dead-worker sweep because the leader had not yet "
    "watched heartbeats for a full TTL (after an outage, failed pass or leadership change).",
)

# Admission controller metrics.
#
# `failed_rule` is deliberately the *rule name* that failed, not the free-text
# `reason` -- reason strings are unbounded (they embed specific numbers and
# timestamps), which would make this label an unbounded-cardinality footgun.
# Rule names are a small, fixed set plus "schedulability" and "none".
scheduler_admission_decisions_total = Counter(
    "scheduler_admission_decisions_total",
    "Admission decisions made, labeled by outcome and which rule (if any) failed.",
    ["outcome", "failed_rule"],
)

scheduler_admission_evaluation_seconds = Histogram(
    "scheduler_admission_evaluation_seconds",
    "Latency of a single admission evaluation (in-process, no I/O).",
)

# Task execution metrics.
#
# On Vercel Functions each instance keeps its own registry, so these are
# per-instance counters; the durable source of truth for task outcomes is the
# tasks / task_transitions tables (see docs/vercel-deployment.md).
task_submissions_total = Counter(
    "nexus_task_submissions_total",
    "Task submissions by outcome (created, duplicate, rejected).",
    ["outcome"],
)

task_claims_total = Counter(
    "nexus_task_claims_total",
    "Dispatch deliveries by claim outcome (claimed, stale, deferred, ...).",
    ["outcome"],
)

task_executions_total = Counter(
    "nexus_task_executions_total",
    "Task attempts executed, by task type and outcome.",
    ["task_type", "outcome"],
)

task_execution_seconds = Histogram(
    "nexus_task_execution_seconds",
    "Wall-clock duration of a task attempt's handler.",
    ["task_type"],
)

reconciler_repairs_total = Counter(
    "nexus_reconciler_repairs_total",
    "Repairs made by the reconciler, by kind.",
    ["kind"],
)

outbox_relayed_total = Counter(
    "nexus_outbox_relayed_total",
    "Outbox rows handled by a relay pass, by trigger (eager, sweep, daemon).",
    ["trigger"],
)

job_submissions_total = Counter(
    "nexus_job_submissions_total", "Job submissions by outcome.", ["outcome"]
)
job_executions_total = Counter(
    "nexus_job_executions_total",
    "Job attempts executed by type and outcome.",
    ["job_type", "outcome"],
)
job_recoveries_total = Counter(
    "nexus_job_recoveries_total", "Jobs recovered by the scheduler, by kind.", ["kind"]
)
rate_limited_requests_total = Counter(
    "nexus_rate_limited_requests_total", "Requests rejected by the rate limiter.", ["scope"]
)
worker_self_fences_total = Counter(
    "nexus_worker_self_fences_total",
    "Times a worker revoked its own execution authority and cancelled its running handlers.",
    ["reason"],
)

worker_handbacks_total = Counter(
    "nexus_worker_handbacks_total",
    "Attempts a worker handed back after losing authority (recorded as lost, or already "
    "recovered by the scheduler).",
    ["result"],
)

dead_letter_pruned_total = Counter(
    "nexus_dead_letter_pruned_total",
    "Broker dead-letter messages removed because their job is no longer dead-lettered.",
)
