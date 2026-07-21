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
