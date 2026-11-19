/**
 * In-browser stand-in for the NEXUS backend, used only in demo mode.
 *
 * It keeps jobs, transitions, executions and workers in memory and advances
 * them with the same state machine as the real domain model
 * (src/nexus/domain/job.py): PENDING → QUEUED → RUNNING → COMPLETED, and
 * RUNNING → FAILED → RETRYING → QUEUED with exponential backoff, ending
 * dead-lettered when attempts run out. Nothing here is executed by real
 * workers: outcomes are decided by the simulator, and every simulated
 * transition says so in its reason.
 *
 * Time only moves when the store is read (`advance()`), so a hidden tab does
 * no work, and the clock is injectable for tests.
 */
import type {
  BulkRetryInput,
  BulkRetryResult,
  CreateJobInput,
  Execution,
  ExecutionStatus,
  Job,
  JobDetail,
  JobPriority,
  JobState,
  Paged,
  QueueItem,
  QueueSnapshot,
  Summary,
  SystemInfo,
  Transition,
  Worker,
} from '../lib/types'
import { Random } from './random'

export const DEMO_TENANT = 'demo'
export const JOB_TYPES = ['echo', 'http.webhook', 'test.fail', 'test.flaky', 'test.sleep']
const PRIORITIES: JobPriority[] = ['low', 'normal', 'high', 'critical']
const PRIORITY_RANK: Record<JobPriority, number> = { critical: 0, high: 1, normal: 2, low: 3 }
const CANCELLABLE: JobState[] = ['pending', 'queued', 'retrying', 'running']
const SIMULATED = ' (simulated)'

const HEARTBEAT_TTL_S = 30
const DISPATCH_DELAY_MS = 600 // outbox relay + broker confirm
const PICKUP_DELAY_MS = 250 // broker → worker
const SPAWN_EVERY_MS = 9000 // background traffic so the dashboard is never static
const MAX_JOBS = 600 // memory bound for long-lived tabs
export const HISTORY_HOURS = 24

/** Error and outcome script for job types whose real handler would call out. */
interface Script {
  failUntilAttempt?: number
  error?: string
}

interface Template {
  name: string
  type: string
  payload: () => Record<string, unknown>
  priority: JobPriority
  maxAttempts: number
  weight: number
  script?: Script
}

interface DemoJob extends Job {
  initial_backoff_seconds: number
  max_backoff_seconds: number
  retry_limit: number
  idempotency_key: string | null
  dispatch_requested_at: string | null
  transitions: Transition[]
  /** Epoch ms of the next simulator step, or null when waiting / final. */
  nextAt: number | null
  script: Script | null
  executionId: number | null
  /** How long the current attempt's handler would take, in ms. */
  plannedMs: number
}

interface DemoWorker {
  worker_id: string
  capacity: number
  registeredAt: number
  /** live: heartbeating · stopped: heartbeat ceased at `stoppedAt` · draining */
  mode: 'live' | 'stopped' | 'draining'
  stoppedAt?: number
  capabilities: Worker['capabilities']
}

export class DemoError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

const iso = (ms: number) => new Date(ms).toISOString()

export class DemoStore {
  private readonly clock: () => number
  private readonly seed: number
  private rnd!: Random
  private jobs: DemoJob[] = []
  private executions: Execution[] = []
  private workers: DemoWorker[] = []
  private idempotency = new Map<string, string>()
  private nextExecutionId = 1
  private counters = new Map<string, number>()
  private lastAdvance = 0
  private nextSpawnAt = 0
  private templates: Template[] = []

  constructor(options: { clock?: () => number; seed?: number } = {}) {
    this.clock = options.clock ?? (() => Date.now())
    this.seed = options.seed ?? 20261119
    this.reset()
  }

  // --- setup ---------------------------------------------------------------------

  reset(): void {
    const now = this.clock()
    this.rnd = new Random(this.seed)
    this.jobs = []
    this.executions = []
    this.idempotency.clear()
    this.nextExecutionId = 1
    this.counters.clear()
    this.templates = this.buildTemplates()
    this.workers = [
      this.worker(4, 'x86_64', 'us-east-1', 8192, now - 26 * 3600e3, 'live'),
      this.worker(4, 'x86_64', 'eu-west-1', 8192, now - 26 * 3600e3, 'live'),
      this.worker(2, 'arm64', 'us-east-1', 4096, now - 9 * 3600e3, 'live'),
      this.worker(2, 'x86_64', 'us-west-2', 4096, now - 30 * 3600e3, 'stopped', now - 7 * 60e3),
    ]
    this.seedHistory(now)
    // Simulate the last minute live, so the first screen already shows work in flight.
    this.lastAdvance = now - 60e3
    this.nextSpawnAt = now - 58e3
    this.seedInFlight(now - 60e3)
    this.advance()
  }

  private worker(
    capacity: number,
    architecture: string,
    region: string,
    memory: number,
    registeredAt: number,
    mode: DemoWorker['mode'],
    stoppedAt?: number,
  ): DemoWorker {
    return {
      worker_id: this.rnd.uuid(),
      capacity,
      registeredAt,
      mode,
      stoppedAt,
      capabilities: {
        cpu_cores: capacity,
        memory_mb: memory,
        architecture,
        region,
        runtime: 'python3.12 (simulated)',
      },
    }
  }

  private buildTemplates(): Template[] {
    const r = this.rnd
    const sha = () => Array.from({ length: 7 }, () => r.int(0, 15).toString(16)).join('')
    return [
      { name: 'build/api-gateway', type: 'echo', payload: () => ({ repo: 'platform/api-gateway', commit: sha() }), priority: 'high', maxAttempts: 3, weight: 14 },
      { name: 'invoice-pdf/render', type: 'echo', payload: () => ({ invoice: `INV-${r.int(20400, 20999)}` }), priority: 'normal', maxAttempts: 3, weight: 14 },
      { name: 'deploy-notify/slack', type: 'http.webhook', payload: () => ({ url: 'https://hooks.example.com/deploy', text: `deployed ${sha()}` }), priority: 'normal', maxAttempts: 3, weight: 11 },
      { name: 'webhook/github-push', type: 'http.webhook', payload: () => ({ url: 'https://ci.example.com/hooks/push', ref: 'refs/heads/main' }), priority: 'high', maxAttempts: 3, weight: 10 },
      { name: 'cleanup/tmp-artifacts', type: 'echo', payload: () => ({ older_than_days: 7 }), priority: 'low', maxAttempts: 3, weight: 8 },
      { name: 'reindex-search/catalog', type: 'test.sleep', payload: () => ({ seconds: r.int(2, 4) }), priority: 'normal', maxAttempts: 3, weight: 8 },
      { name: 'nightly-backup/orders-db', type: 'test.sleep', payload: () => ({ seconds: r.int(5, 9) }), priority: 'low', maxAttempts: 3, weight: 6 },
      { name: 'export-report/weekly', type: 'test.sleep', payload: () => ({ seconds: 2 }), priority: 'normal', maxAttempts: 3, weight: 5 },
      // Recovers on its second attempt: an automatic retry with backoff.
      { name: 'rotate-certificates/edge', type: 'test.flaky', payload: () => ({ succeed_on_attempt: 2 }), priority: 'critical', maxAttempts: 3, weight: 4 },
      // Exhausts its attempts and is dead-lettered; a manual retry succeeds.
      {
        name: 'sync-crm/contacts', type: 'http.webhook', payload: () => ({ url: 'https://crm.example.com/api/sync' }), priority: 'high', maxAttempts: 3, weight: 2,
        script: { failUntilAttempt: 4, error: "HTTPStatusError: Server error '503 Service Unavailable' for url 'https://crm.example.com/api/sync'" },
      },
      { name: 'sync-ledger/eu', type: 'test.flaky', payload: () => ({ succeed_on_attempt: 3 }), priority: 'normal', maxAttempts: 2, weight: 1.5 },
      // Permanent error: dead-lettered after one attempt, and a retry fails again.
      { name: 'scan-images/registry', type: 'test.fail', payload: () => ({ message: 'registry returned 403 for ghcr.io/acme/api', permanent: true }), priority: 'normal', maxAttempts: 3, weight: 1.5 },
    ]
  }

  private pickTemplate(): Template {
    const total = this.templates.reduce((s, t) => s + t.weight, 0)
    let x = this.rnd.next() * total
    for (const t of this.templates) {
      x -= t.weight
      if (x <= 0) return t
    }
    return this.templates[0]
  }

  private fromTemplate(t: Template, at: number): DemoJob {
    const n = (this.counters.get(t.name) ?? 400 + this.rnd.int(0, 300)) + 1
    this.counters.set(t.name, n)
    return this.newJob(
      { name: `${t.name} #${n}`, job_type: t.type, payload: t.payload(), priority: t.priority, max_attempts: t.maxAttempts, initial_backoff_seconds: 2, max_backoff_seconds: 30 },
      at,
      t.script ?? null,
    )
  }

  /** 24 hours of finished work, produced by running each job to completion. */
  private seedHistory(now: number): void {
    const start = now - HISTORY_HOURS * 3600e3
    const end = now - 90e3
    const count = 170
    const times = Array.from({ length: count }, () => {
      // Busier during the working day of the last few hours.
      const u = this.rnd.next()
      return start + (end - start) * Math.pow(u, 0.8)
    }).sort((a, b) => a - b)
    const live = this.workers.filter((w) => w.mode !== 'stopped')
    const stopped = this.workers.find((w) => w.mode === 'stopped')
    let turn = 0
    for (const at of times) {
      const job = this.fromTemplate(this.pickTemplate(), at)
      if (this.rnd.chance(0.02)) {
        this.step(job, at + DISPATCH_DELAY_MS) // queued
        this.cancel(job, at + 1500, 'cancelled by user')
        continue
      }
      // Run the job alone to completion, handing attempts to workers in turn.
      let guard = 0
      while (job.nextAt !== null && guard++ < 50) {
        const t = job.nextAt
        if (job.state === 'queued') {
          const pool = stopped && stopped.stoppedAt && t < stopped.stoppedAt ? [...live, stopped] : live
          this.start(job, pool[turn++ % pool.length], t)
        } else {
          this.step(job, t)
        }
      }
    }
  }

  private seedInFlight(at: number): void {
    // A long backup that is still running when the page opens.
    const backup = this.templates.find((t) => t.name.startsWith('nightly-backup'))!
    const job = this.fromTemplate({ ...backup, payload: () => ({ seconds: 140 }) }, at - 20e3)
    job.timeout_seconds = 300
  }

  // --- simulation ------------------------------------------------------------------

  /** Advance the simulation to the current time. Called on every read. */
  advance(): void {
    const now = this.clock()
    if (now - this.lastAdvance > 10 * 60e3) {
      // Tab was asleep: skip the gap instead of replaying hours of traffic.
      this.nextSpawnAt = Math.max(this.nextSpawnAt, now - SPAWN_EVERY_MS)
    }
    for (let guard = 0; guard < 5000; guard++) {
      let job: DemoJob | null = null
      for (const j of this.jobs) {
        if (j.nextAt !== null && j.nextAt <= now && (job === null || j.nextAt < (job.nextAt as number))) job = j
      }
      const spawnDue = this.nextSpawnAt <= now
      if (!job && !spawnDue) break
      if (spawnDue && (!job || this.nextSpawnAt <= (job.nextAt as number))) {
        this.fromTemplate(this.pickTemplate(), this.nextSpawnAt)
        this.nextSpawnAt += SPAWN_EVERY_MS * (0.6 + this.rnd.next() * 0.8)
        continue
      }
      this.step(job as DemoJob, job!.nextAt as number)
    }
    this.lastAdvance = now
    this.trim()
  }

  private trim(): void {
    if (this.jobs.length <= MAX_JOBS) return
    const final = this.jobs
      .filter((j) => (j.state === 'completed' || j.state === 'cancelled') && j.nextAt === null)
      .sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at))
    const drop = new Set(final.slice(0, this.jobs.length - MAX_JOBS).map((j) => j.job_id))
    this.jobs = this.jobs.filter((j) => !drop.has(j.job_id))
    this.executions = this.executions.filter((e) => !drop.has(e.job_id))
  }

  private transition(job: DemoJob, to: JobState, at: number, reason: string | null): void {
    job.transitions.push({ from_state: job.state, to_state: to, occurred_at: iso(at), attempt: job.attempt, reason })
    job.state = to
    job.updated_at = iso(at)
    job.can_cancel = CANCELLABLE.includes(to)
    job.can_retry = to === 'failed'
    job.attempts_remaining = Math.max(job.max_attempts - job.attempt, 0)
  }

  /** One simulator step for the job's current state. */
  private step(job: DemoJob, at: number): void {
    switch (job.state) {
      case 'pending':
      case 'retrying':
        this.transition(job, 'queued', at, 'dispatch confirmed by broker' + SIMULATED)
        job.queued_at = iso(at)
        job.next_attempt_at = null
        job.nextAt = at + PICKUP_DELAY_MS
        return
      case 'queued': {
        const worker = this.freeWorker(at)
        if (worker) this.start(job, worker, at)
        else job.nextAt = null // waits for a free slot; finish() wakes it
        return
      }
      case 'running':
        this.finish(job, at)
        return
      default:
        job.nextAt = null
    }
  }

  private running(workerId: string): number {
    return this.jobs.filter((j) => j.state === 'running' && j.worker_id === workerId).length
  }

  private freeWorker(at: number): DemoWorker | null {
    let best: DemoWorker | null = null
    let bestLoad = Infinity
    for (const w of this.workers) {
      if (this.workerStatus(w, at) !== 'healthy') continue
      const load = this.running(w.worker_id) / w.capacity
      if (load < 1 && load < bestLoad) {
        best = w
        bestLoad = load
      }
    }
    return best
  }

  private start(job: DemoJob, worker: DemoWorker, at: number): void {
    this.transition(job, 'running', at, `started by worker ${worker.worker_id}` + SIMULATED)
    job.worker_id = worker.worker_id
    job.started_at = iso(at)
    job.finished_at = null
    const execution: Execution = {
      execution_id: this.nextExecutionId++,
      job_id: job.job_id,
      job_name: job.name,
      attempt: job.attempt,
      worker_id: worker.worker_id,
      status: 'running',
      started_at: iso(at),
      finished_at: null,
      duration_ms: null,
      error: null,
    }
    this.executions.push(execution)
    job.executionId = execution.execution_id
    job.plannedMs = this.duration(job)
    job.nextAt = at + Math.min(job.plannedMs, job.timeout_seconds * 1000)
  }

  private duration(job: DemoJob): number {
    const r = this.rnd
    switch (job.job_type) {
      case 'test.sleep':
        return Number(job.payload.seconds ?? 1) * 1000 + r.int(20, 120)
      case 'http.webhook':
        return r.int(180, 1400)
      case 'echo':
        return r.int(40, 400)
      default:
        return r.int(150, 600)
    }
  }

  /** Decide the attempt's outcome the way the real handler would. */
  private outcome(job: DemoJob): { ok: true; result: unknown } | { ok: false; error: string; permanent: boolean } {
    const a = job.attempt
    if (job.script?.failUntilAttempt && a < job.script.failUntilAttempt) {
      return { ok: false, error: job.script.error ?? 'RuntimeError: simulated failure', permanent: false }
    }
    switch (job.job_type) {
      case 'test.flaky': {
        const succeedOn = Number(job.payload.succeed_on_attempt ?? 2)
        return a >= succeedOn ? { ok: true, result: { attempt: a } } : { ok: false, error: `RuntimeError: flaky failure on attempt ${a}`, permanent: false }
      }
      case 'test.fail': {
        const message = String(job.payload.message ?? 'requested failure')
        return job.payload.permanent
          ? { ok: false, error: `PermanentTaskError: ${message}`, permanent: true }
          : { ok: false, error: `RuntimeError: ${message}`, permanent: false }
      }
      case 'test.sleep':
        return { ok: true, result: { slept: Number(job.payload.seconds ?? 1) } }
      case 'http.webhook':
        return { ok: true, result: { status: 200, simulated: true } }
      default:
        return { ok: true, result: { echo: job.payload, attempt: a } }
    }
  }

  private closeExecution(job: DemoJob, at: number, status: ExecutionStatus, error: string | null): void {
    const e = this.executions.find((x) => x.execution_id === job.executionId)
    if (!e) return
    e.status = status
    e.finished_at = iso(at)
    e.duration_ms = Math.max(Date.parse(e.finished_at) - Date.parse(e.started_at), 0)
    e.error = error
    job.executionId = null
  }

  private finish(job: DemoJob, at: number): void {
    const timedOut = job.plannedMs > job.timeout_seconds * 1000
    const result = timedOut
      ? ({ ok: false, error: `TimeoutError: attempt exceeded ${job.timeout_seconds}s`, permanent: false } as const)
      : this.outcome(job)
    if (result.ok) {
      this.closeExecution(job, at, 'completed', null)
      this.transition(job, 'completed', at, 'completed')
      job.result = result.result
      job.last_error = null
      job.finished_at = iso(at)
      job.nextAt = null
    } else {
      this.closeExecution(job, at, timedOut ? 'timed_out' : 'failed', result.error)
      this.fail(job, at, result.error, result.permanent)
    }
    this.wakeWaiting(at)
  }

  private fail(job: DemoJob, at: number, error: string, permanent: boolean): void {
    this.transition(job, 'failed', at, error)
    job.last_error = error
    job.finished_at = iso(at)
    if (permanent || job.attempt >= job.max_attempts) {
      const reason = permanent ? 'permanent error' : `attempts exhausted (${job.attempt})`
      job.dead_lettered_at = iso(at)
      job.next_attempt_at = null
      job.last_error = `${error} [${reason}]`
      job.nextAt = null
      return
    }
    const backoff = Math.min(job.initial_backoff_seconds * 2 ** (job.attempt - 1), job.max_backoff_seconds)
    const next = job.attempt + 1
    job.attempt = next
    this.transition(job, 'retrying', at, `retry ${next}/${job.max_attempts} in ${backoff}s`)
    job.transitions[job.transitions.length - 1].attempt = next - 1
    job.worker_id = null
    job.next_attempt_at = iso(at + backoff * 1000)
    job.nextAt = at + backoff * 1000
  }

  private wakeWaiting(at: number): void {
    const waiting = this.jobs
      .filter((j) => j.state === 'queued' && j.nextAt === null)
      .sort((a, b) => PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority] || Date.parse(a.created_at) - Date.parse(b.created_at))
    for (const j of waiting) j.nextAt = at + PICKUP_DELAY_MS
  }

  private newJob(input: CreateJobInput, at: number, script: Script | null, idempotencyKey: string | null = null): DemoJob {
    const maxAttempts = input.max_attempts ?? 3
    const job: DemoJob = {
      job_id: this.rnd.uuid(),
      name: input.name,
      job_type: input.job_type,
      priority: input.priority,
      state: 'pending',
      attempt: 1,
      max_attempts: maxAttempts,
      attempts_remaining: maxAttempts - 1,
      timeout_seconds: input.timeout_seconds ?? 60,
      payload: input.payload,
      result: null,
      last_error: null,
      worker_id: null,
      correlation_id: this.rnd.uuid().replaceAll('-', ''),
      created_at: iso(at),
      updated_at: iso(at),
      queued_at: null,
      started_at: null,
      finished_at: null,
      next_attempt_at: null,
      dead_lettered_at: null,
      can_cancel: true,
      can_retry: false,
      initial_backoff_seconds: input.initial_backoff_seconds ?? 2,
      max_backoff_seconds: input.max_backoff_seconds ?? 30,
      retry_limit: maxAttempts,
      idempotency_key: idempotencyKey,
      dispatch_requested_at: iso(at),
      transitions: [],
      nextAt: at + DISPATCH_DELAY_MS,
      script,
      executionId: null,
      plannedMs: 0,
    }
    this.jobs.push(job)
    return job
  }

  private cancel(job: DemoJob, at: number, reason: string): void {
    if (job.state === 'running') {
      this.closeExecution(job, at, 'cancelled', reason)
      job.last_error = reason
    }
    this.transition(job, 'cancelled', at, reason)
    job.finished_at = iso(at)
    job.nextAt = null
    this.wakeWaiting(at)
  }

  private retryManually(job: DemoJob, at: number): void {
    if (job.state !== 'failed') {
      throw new DemoError(409, 'not_retryable', `only failed jobs can be retried (job is ${job.state})`)
    }
    const next = job.attempt + 1
    job.transitions.push({ from_state: 'failed', to_state: 'retrying', occurred_at: iso(at), attempt: next, reason: 'manual retry' })
    job.state = 'retrying'
    job.attempt = next
    job.max_attempts = job.attempt - 1 + job.retry_limit
    job.attempts_remaining = job.max_attempts - job.attempt
    job.dead_lettered_at = null
    job.worker_id = null
    job.next_attempt_at = iso(at)
    job.updated_at = iso(at)
    job.can_cancel = true
    job.can_retry = false
    job.nextAt = at + DISPATCH_DELAY_MS
  }

  // --- read models -----------------------------------------------------------------

  private toJob(j: DemoJob): Job {
    return {
      job_id: j.job_id,
      name: j.name,
      job_type: j.job_type,
      priority: j.priority,
      state: j.state,
      attempt: j.attempt,
      max_attempts: j.max_attempts,
      attempts_remaining: j.attempts_remaining,
      timeout_seconds: j.timeout_seconds,
      payload: j.payload,
      result: j.result,
      last_error: j.last_error,
      worker_id: j.worker_id,
      correlation_id: j.correlation_id,
      created_at: j.created_at,
      updated_at: j.updated_at,
      queued_at: j.queued_at,
      started_at: j.started_at,
      finished_at: j.finished_at,
      next_attempt_at: j.next_attempt_at,
      dead_lettered_at: j.dead_lettered_at,
      can_cancel: j.can_cancel,
      can_retry: j.can_retry,
    }
  }

  private find(jobId: string): DemoJob {
    const job = this.jobs.find((j) => j.job_id === jobId)
    if (!job) throw new DemoError(404, 'not_found', 'job not found')
    return job
  }

  private workerStatus(w: DemoWorker, at: number): Worker['status'] {
    if (w.mode === 'draining') return 'draining'
    if (w.mode === 'stopped' && w.stoppedAt !== undefined && at - w.stoppedAt > HEARTBEAT_TTL_S * 1000) return 'unhealthy'
    return 'healthy'
  }

  private page<T>(items: T[], page: number, pageSize: number): Paged<T> {
    const total = items.length
    return {
      items: items.slice((page - 1) * pageSize, page * pageSize),
      total,
      page,
      page_size: pageSize,
      pages: Math.max(Math.ceil(total / pageSize), 1),
    }
  }

  private filterJobs(f: { status?: string[]; priority?: string[]; q?: string; deadLettered?: boolean | null; error?: string }): DemoJob[] {
    const q = f.q?.trim().toLowerCase()
    const error = f.error?.trim().toLowerCase()
    return this.jobs
      .filter(
        (j) =>
          (!f.status?.length || f.status.includes(j.state)) &&
          (!f.priority?.length || f.priority.includes(j.priority)) &&
          (f.deadLettered == null || (j.dead_lettered_at !== null) === f.deadLettered) &&
          (!q || j.name.toLowerCase().includes(q) || j.job_type.toLowerCase().includes(q) || j.job_id.includes(q)) &&
          (!error || (j.last_error ?? '').toLowerCase().includes(error)),
      )
      .sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at))
  }

  listJobs(f: { status?: string[]; priority?: string[]; q?: string; deadLettered?: boolean | null; error?: string; page: number; pageSize: number }): Paged<Job> {
    this.advance()
    const result = this.page(this.filterJobs(f), f.page, f.pageSize)
    return { ...result, items: result.items.map((j) => this.toJob(j as DemoJob)) }
  }

  getJob(jobId: string): JobDetail {
    this.advance()
    const job = this.find(jobId)
    return {
      job: this.toJob(job),
      transitions: [...job.transitions],
      executions: this.executions.filter((e) => e.job_id === jobId).sort((a, b) => b.attempt - a.attempt),
    }
  }

  createJob(input: CreateJobInput, idempotencyKey: string | null): { job: Job; created: boolean } {
    this.advance()
    if (idempotencyKey && this.idempotency.has(idempotencyKey)) {
      return { job: this.toJob(this.find(this.idempotency.get(idempotencyKey)!)), created: false }
    }
    if (!input.name?.trim() || input.name.length > 200) throw new DemoError(422, 'validation_error', 'name: must be 1–200 characters')
    if (!JOB_TYPES.includes(input.job_type)) throw new DemoError(422, 'job_rejected', `job_type: unknown job type '${input.job_type}'`)
    if (!PRIORITIES.includes(input.priority)) throw new DemoError(422, 'validation_error', 'priority: invalid')
    const attempts = input.max_attempts ?? 3
    if (!Number.isInteger(attempts) || attempts < 1 || attempts > 20) throw new DemoError(422, 'validation_error', 'max_attempts: between 1 and 20')
    if (input.timeout_seconds !== undefined && (input.timeout_seconds < 1 || input.timeout_seconds > 3600)) {
      throw new DemoError(422, 'validation_error', 'timeout_seconds: between 1 and 3600')
    }
    const job = this.newJob(input, this.clock(), null, idempotencyKey)
    if (idempotencyKey) this.idempotency.set(idempotencyKey, job.job_id)
    return { job: this.toJob(job), created: true }
  }

  cancelJob(jobId: string, reason = 'cancelled by user'): Job {
    this.advance()
    const job = this.find(jobId)
    if (!CANCELLABLE.includes(job.state)) throw new DemoError(409, 'not_cancellable', `a ${job.state} job cannot be cancelled`)
    this.cancel(job, this.clock(), reason)
    return this.toJob(job)
  }

  retryJob(jobId: string): Job {
    this.advance()
    const job = this.find(jobId)
    this.retryManually(job, this.clock())
    return this.toJob(job)
  }

  bulkRetry(input: BulkRetryInput): BulkRetryResult {
    this.advance()
    const byIds = 'job_ids' in input
    if (byIds === ('dead_lettered' in input && input.dead_lettered === true)) {
      throw new DemoError(422, 'validation_error', 'give either job_ids or dead_lettered=true')
    }
    let ids: string[]
    if (byIds) {
      if (!input.job_ids.length || input.job_ids.length > 100) throw new DemoError(422, 'validation_error', 'job_ids: 1–100 ids')
      ids = input.job_ids
    } else {
      ids = this.filterJobs({ status: ['failed'], deadLettered: true, error: input.error, q: input.q })
        .slice(0, input.limit ?? 100)
        .map((j) => j.job_id)
    }
    const retried: Job[] = []
    const skipped: BulkRetryResult['skipped'] = []
    const now = this.clock()
    for (const id of [...new Set(ids)]) {
      const job = this.jobs.find((j) => j.job_id === id)
      if (!job) skipped.push({ job_id: id, code: 'not_found', message: 'job not found' })
      else if (job.state !== 'failed') skipped.push({ job_id: id, code: 'not_retryable', message: `only failed jobs can be retried (job is ${job.state})` })
      else {
        this.retryManually(job, now)
        retried.push(this.toJob(job))
      }
    }
    return { requested: ids.length, retried, skipped }
  }

  listExecutions(f: { status?: string; jobId?: string; page: number; pageSize: number }): Paged<Execution> {
    this.advance()
    const items = this.executions
      .filter((e) => (!f.status || e.status === f.status) && (!f.jobId || e.job_id === f.jobId))
      .sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at))
    return this.page(items.map((e) => ({ ...e })), f.page, f.pageSize)
  }

  listWorkers(): Worker[] {
    this.advance()
    const now = this.clock()
    return this.workers.map((w) => {
      // Live workers heartbeat every 5 s; a stopped one keeps its last beat.
      const last = w.mode === 'stopped' && w.stoppedAt !== undefined ? w.stoppedAt : now - ((now - w.registeredAt) % 5000)
      const status = this.workerStatus(w, now)
      const busy = this.running(w.worker_id)
      return {
        worker_id: w.worker_id,
        status,
        healthy: status === 'healthy',
        last_heartbeat_at: iso(last),
        heartbeat_age_seconds: Math.round((now - last) / 100) / 10,
        registered_at: iso(w.registeredAt),
        capacity_slots: w.capacity,
        running_jobs: busy,
        utilization: Math.round((busy / w.capacity) * 100) / 100,
        capabilities: { ...w.capabilities },
      }
    })
  }

  private counts(): Record<JobState, number> {
    const c: Record<JobState, number> = { pending: 0, queued: 0, running: 0, completed: 0, failed: 0, retrying: 0, cancelled: 0 }
    for (const j of this.jobs) c[j.state]++
    return c
  }

  private broker(): Summary['broker'] {
    const now = this.clock()
    return {
      available: true,
      ready_messages: this.jobs.filter((j) => j.state === 'queued').length,
      consumers: this.workers.filter((w) => this.workerStatus(w, now) === 'healthy').length,
      dead_letter_messages: this.jobs.filter((j) => j.dead_lettered_at !== null).length,
    }
  }

  queue(limit = 100): QueueSnapshot {
    this.advance()
    const now = this.clock()
    const c = this.counts()
    const waiting = this.jobs
      .filter((j) => j.state === 'pending' || j.state === 'queued' || j.state === 'retrying')
      .sort((a, b) => PRIORITY_RANK[a.priority] - PRIORITY_RANK[b.priority] || Date.parse(a.created_at) - Date.parse(b.created_at))
    const items: QueueItem[] = waiting.slice(0, limit).map((j) => ({
      job: this.toJob(j),
      dispatch_status:
        j.state === 'queued'
          ? 'confirmed'
          : j.state === 'retrying' && j.next_attempt_at && Date.parse(j.next_attempt_at) > now
            ? 'scheduled_retry'
            : 'awaiting_publish',
      dispatch_requested_at: j.dispatch_requested_at,
    }))
    return {
      pending: c.pending,
      queued: c.queued,
      retrying: c.retrying,
      depth: c.pending + c.queued + c.retrying,
      dead_lettered_jobs: this.jobs.filter((j) => j.dead_lettered_at !== null).length,
      broker: this.broker(),
      unpublished_events: items.filter((i) => i.dispatch_status === 'awaiting_publish').length,
      items,
    }
  }

  summary(hours = 24): Summary {
    const workers = this.listWorkers() // advances
    const now = this.clock()
    const c = this.counts()
    const hour = 3600e3
    const since = Math.floor((now - hours * hour) / hour) * hour
    const trend: Summary['trend'] = []
    for (let b = since; b <= now; b += hour) trend.push({ bucket_start: iso(b), completed: 0, failed: 0 })
    for (const e of this.executions) {
      if (!e.finished_at) continue
      const t = Date.parse(e.finished_at)
      if (t < since) continue
      const bucket = trend[Math.floor((t - since) / hour)]
      if (!bucket) continue
      if (e.status === 'completed') bucket.completed++
      else if (e.status === 'failed' || e.status === 'timed_out' || e.status === 'lost') bucket.failed++
    }
    const activity = this.jobs
      .flatMap((j) => j.transitions.map((t) => ({ ...t, job_id: j.job_id, job_name: j.name })))
      .sort((a, b) => Date.parse(b.occurred_at) - Date.parse(a.occurred_at))
      .slice(0, 15)
    return {
      total_jobs: this.jobs.length,
      by_state: c,
      running: c.running,
      completed: c.completed,
      failed: c.failed,
      dead_lettered: this.jobs.filter((j) => j.dead_lettered_at !== null).length,
      queue_depth: c.pending + c.queued + c.retrying,
      broker: this.broker(),
      workers: {
        total: workers.length,
        healthy: workers.filter((w) => w.status === 'healthy').length,
        unhealthy: workers.filter((w) => w.status === 'unhealthy').length,
        draining: workers.filter((w) => w.status === 'draining').length,
        capacity: workers.filter((w) => w.status === 'healthy').reduce((s, w) => s + w.capacity_slots, 0),
        busy: workers.reduce((s, w) => s + w.running_jobs, 0),
      },
      recent_activity: activity,
      trend,
      generated_at: iso(now),
    }
  }

  system(): SystemInfo {
    return {
      version: '0.2.0',
      environment: 'demo',
      tenant_id: DEMO_TENANT,
      event_bus: 'rabbitmq (simulated in browser)',
      checks: { postgres: 'simulated', rabbitmq: 'simulated', redis: 'simulated' },
      job_types: [...JOB_TYPES],
      config: {
        job_default_timeout_seconds: 60,
        job_max_timeout_seconds: 3600,
        job_default_max_attempts: 3,
        worker_heartbeat_ttl_seconds: HEARTBEAT_TTL_S,
        data_source: 'in-browser simulator (no backend)',
      },
    }
  }
}
