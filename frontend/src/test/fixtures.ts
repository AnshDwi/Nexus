import type { Execution, Job, JobState, Summary, SystemInfo, Worker } from '../lib/types'

let seq = 0
export function makeJob(overrides: Partial<Job> = {}): Job {
  seq += 1
  const state: JobState = overrides.state ?? 'completed'
  return {
    job_id: overrides.job_id ?? `00000000-0000-4000-8000-${String(seq).padStart(12, '0')}`,
    name: `job-${seq}`,
    job_type: 'echo',
    priority: 'normal',
    state,
    attempt: 1,
    max_attempts: 3,
    attempts_remaining: 2,
    timeout_seconds: 60,
    payload: {},
    result: null,
    last_error: null,
    worker_id: null,
    correlation_id: 'corr',
    created_at: new Date(Date.now() - 60_000).toISOString(),
    updated_at: new Date().toISOString(),
    queued_at: null,
    started_at: null,
    finished_at: null,
    next_attempt_at: null,
    dead_lettered_at: null,
    can_cancel: ['pending', 'queued', 'retrying', 'running'].includes(state),
    can_retry: state === 'failed',
    ...overrides,
  }
}

export const summary: Summary = {
  total_jobs: 42,
  by_state: { pending: 1, queued: 2, running: 3, completed: 30, failed: 4, retrying: 1, cancelled: 1 },
  running: 3,
  completed: 30,
  failed: 4,
  dead_lettered: 2,
  queue_depth: 4,
  broker: { available: true, ready_messages: 2, consumers: 1, dead_letter_messages: 1 },
  workers: { total: 2, healthy: 2, unhealthy: 0, draining: 0, capacity: 8, busy: 3 },
  recent_activity: [
    {
      job_id: 'j1',
      job_name: 'nightly-report',
      from_state: 'running',
      to_state: 'completed',
      occurred_at: new Date().toISOString(),
      attempt: 1,
      reason: 'completed',
    },
  ],
  trend: [{ bucket_start: new Date().toISOString(), completed: 5, failed: 1 }],
  generated_at: new Date().toISOString(),
}

export const worker: Worker = {
  worker_id: '11111111-2222-4333-8444-555555555555',
  status: 'healthy',
  healthy: true,
  last_heartbeat_at: new Date().toISOString(),
  heartbeat_age_seconds: 1.2,
  registered_at: new Date().toISOString(),
  capacity_slots: 4,
  running_jobs: 1,
  utilization: 0.25,
  capabilities: { cpu_cores: 8, memory_mb: 16384, architecture: 'x86_64', region: 'iad1', runtime: 'python3.12' },
}

export const system: SystemInfo = {
  version: '0.2.0',
  environment: 'test',
  tenant_id: 'acme',
  event_bus: 'rabbitmq',
  checks: { postgres: 'ok', redis: 'not_configured', rabbitmq: 'ok' },
  job_types: ['echo', 'test.fail'],
  config: { job_default_max_attempts: 3, cors_allowed_origins: [] },
}

export function makeExecution(overrides: Partial<Execution> = {}): Execution {
  return {
    execution_id: 1,
    job_id: 'j1',
    job_name: 'nightly-report',
    attempt: 2,
    worker_id: worker.worker_id,
    status: 'failed',
    started_at: new Date().toISOString(),
    finished_at: new Date().toISOString(),
    duration_ms: 1234,
    error: 'RuntimeError: boom',
    ...overrides,
  }
}
