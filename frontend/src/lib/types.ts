export type JobState =
  | 'pending'
  | 'queued'
  | 'running'
  | 'completed'
  | 'failed'
  | 'retrying'
  | 'cancelled'

export type JobPriority = 'low' | 'normal' | 'high' | 'critical'

export const JOB_STATES: JobState[] = [
  'pending',
  'queued',
  'running',
  'retrying',
  'completed',
  'failed',
  'cancelled',
]
export const JOB_PRIORITIES: JobPriority[] = ['critical', 'high', 'normal', 'low']

export interface Job {
  job_id: string
  name: string
  job_type: string
  priority: JobPriority
  state: JobState
  attempt: number
  max_attempts: number
  attempts_remaining: number
  timeout_seconds: number
  payload: Record<string, unknown>
  result: unknown
  last_error: string | null
  worker_id: string | null
  correlation_id: string | null
  created_at: string
  updated_at: string
  queued_at: string | null
  started_at: string | null
  finished_at: string | null
  next_attempt_at: string | null
  dead_lettered_at: string | null
  can_cancel: boolean
  can_retry: boolean
}

export interface Transition {
  from_state: JobState
  to_state: JobState
  occurred_at: string
  attempt: number
  reason: string | null
}

export type ExecutionStatus = 'running' | 'completed' | 'failed' | 'timed_out' | 'lost' | 'cancelled'

export interface Execution {
  execution_id: number
  job_id: string
  job_name: string
  attempt: number
  worker_id: string | null
  status: ExecutionStatus
  started_at: string
  finished_at: string | null
  duration_ms: number | null
  error: string | null
}

export interface Paged<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface JobDetail {
  job: Job
  transitions: Transition[]
  executions: Execution[]
}

export interface Worker {
  worker_id: string
  status: 'healthy' | 'unhealthy' | 'draining'
  healthy: boolean
  last_heartbeat_at: string
  heartbeat_age_seconds: number
  registered_at: string
  capacity_slots: number
  running_jobs: number
  utilization: number
  capabilities: {
    cpu_cores: number
    memory_mb: number
    architecture: string
    region: string
    runtime: string
  }
}

export interface BrokerDepth {
  available: boolean
  ready_messages: number | null
  consumers: number | null
  dead_letter_messages: number | null
}

export interface QueueItem {
  job: Job
  dispatch_status: 'awaiting_publish' | 'scheduled_retry' | 'confirmed'
  dispatch_requested_at: string | null
}

export interface QueueSnapshot {
  pending: number
  queued: number
  retrying: number
  depth: number
  dead_lettered_jobs: number
  broker: BrokerDepth
  unpublished_events: number
  items: QueueItem[]
}

export interface Summary {
  total_jobs: number
  by_state: Record<JobState, number>
  running: number
  completed: number
  failed: number
  dead_lettered: number
  queue_depth: number
  broker: BrokerDepth
  workers: {
    total: number
    healthy: number
    unhealthy: number
    draining: number
    capacity: number
    busy: number
  }
  recent_activity: Array<{
    job_id: string
    job_name: string
    from_state: JobState
    to_state: JobState
    occurred_at: string
    attempt: number
    reason: string | null
  }>
  trend: Array<{ bucket_start: string; completed: number; failed: number }>
  generated_at: string
}

export interface SystemInfo {
  version: string
  environment: string
  tenant_id: string
  event_bus: string
  checks: Record<string, string>
  job_types: string[]
  config: Record<string, unknown>
}

export interface CreateJobInput {
  name: string
  job_type: string
  payload: Record<string, unknown>
  priority: JobPriority
  max_attempts?: number
  timeout_seconds?: number
  initial_backoff_seconds?: number
  max_backoff_seconds?: number
}

export interface JobFilters {
  status: JobState[]
  priority: JobPriority[]
  q: string
  deadLettered?: boolean
  /** Case-insensitive substring of the job's last error. */
  error?: string
  page: number
  pageSize: number
}

export type BulkRetryInput =
  | { job_ids: string[] }
  | { dead_lettered: true; error?: string; q?: string; limit?: number }

export interface BulkRetryResult {
  requested: number
  retried: Job[]
  skipped: { job_id: string; code: string; message: string }[]
}
