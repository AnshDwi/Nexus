import { describe, expect, it } from 'vitest'
import type { Job } from '../lib/types'
import { DemoError, DemoStore } from './store'

const T0 = Date.parse('2026-11-19T12:00:00Z')

function setup(seed = 7) {
  let now = T0
  const store = new DemoStore({ clock: () => now, seed })
  const tick = (ms: number) => {
    now += ms
  }
  return { store, tick, now: () => now }
}

/** Advance in small steps until the job reaches one of `states`. */
function runUntil(store: DemoStore, tick: (ms: number) => void, jobId: string, states: string[], limitMs = 120_000): Job {
  for (let t = 0; t <= limitMs; t += 100) {
    const { job } = store.getJob(jobId)
    if (states.includes(job.state)) return job
    tick(100)
  }
  throw new Error(`job ${jobId} never reached ${states.join('/')}: ${store.getJob(jobId).job.state}`)
}

const allJobs = (store: DemoStore) => store.listJobs({ page: 1, pageSize: 100_000 }).items

describe('DemoStore seed', () => {
  it('is deterministic for a given seed and clock', () => {
    const a = allJobs(setup(7).store).map((j) => [j.job_id, j.name, j.state])
    const b = allJobs(setup(7).store).map((j) => [j.job_id, j.name, j.state])
    expect(a).toEqual(b)
    expect(allJobs(setup(8).store)[0].job_id).not.toBe(allJobs(setup(7).store)[0].job_id)
  })

  it('seeds a believable day: completions, recoveries, dead letters and an unhealthy worker', () => {
    const { store } = setup()
    const jobs = allJobs(store)
    expect(jobs.length).toBeGreaterThan(150)
    expect(jobs.filter((j) => j.state === 'completed').length).toBeGreaterThan(120)
    const dead = jobs.filter((j) => j.dead_lettered_at)
    expect(dead.length).toBeGreaterThanOrEqual(3)
    expect(dead.every((j) => j.state === 'failed' && j.can_retry && j.next_attempt_at === null)).toBe(true)
    expect(new Set(dead.map((j) => j.last_error?.split(' [')[0])).size).toBeGreaterThan(1)
    // Something is in flight when the page opens.
    expect(jobs.some((j) => j.state === 'running')).toBe(true)

    const workers = store.listWorkers()
    expect(workers.map((w) => w.status).sort()).toEqual(['healthy', 'healthy', 'healthy', 'unhealthy'])
    const unhealthy = workers.find((w) => w.status === 'unhealthy')!
    expect(unhealthy.running_jobs).toBe(0)
    expect(unhealthy.heartbeat_age_seconds).toBeGreaterThan(30)

    const summary = store.summary()
    expect(summary.trend).toHaveLength(25)
    expect(summary.trend.reduce((s, b) => s + b.completed, 0)).toBeGreaterThan(100)
  })

  it('reports consistent numbers across endpoints', () => {
    const { store } = setup()
    const summary = store.summary()
    const jobs = allJobs(store)
    for (const [state, count] of Object.entries(summary.by_state)) {
      expect(jobs.filter((j) => j.state === state)).toHaveLength(count)
    }
    const queue = store.queue()
    expect(queue.dead_lettered_jobs).toBe(summary.dead_lettered)
    expect(queue.broker.dead_letter_messages).toBe(summary.dead_lettered)
    expect(summary.workers.busy).toBe(summary.running)
    expect(summary.workers.capacity).toBe(10) // healthy workers: 4 + 4 + 2
  })
})

describe('DemoStore lifecycle', () => {
  it('moves a job through the real state machine, marking simulated steps', () => {
    const { store, tick } = setup()
    const { job } = store.createJob({ name: 'hello', job_type: 'echo', payload: { msg: 'hi' }, priority: 'high' }, null)
    expect(job.state).toBe('pending')
    const done = runUntil(store, tick, job.job_id, ['completed'])
    expect(done.result).toEqual({ echo: { msg: 'hi' }, attempt: 1 })
    const detail = store.getJob(job.job_id)
    expect(detail.transitions.map((t) => `${t.from_state}>${t.to_state}`)).toEqual([
      'pending>queued',
      'queued>running',
      'running>completed',
    ])
    expect(detail.transitions[0].reason).toBe('dispatch confirmed by broker (simulated)')
    expect(detail.transitions[1].reason).toMatch(/^started by worker .+ \(simulated\)$/)
    expect(detail.executions).toHaveLength(1)
    expect(detail.executions[0]).toMatchObject({ status: 'completed', attempt: 1 })
  })

  it('retries with exponential backoff and recovers', () => {
    const { store, tick } = setup()
    const { job } = store.createJob(
      { name: 'flaky', job_type: 'test.flaky', payload: { succeed_on_attempt: 3 }, priority: 'normal', max_attempts: 3, initial_backoff_seconds: 1, max_backoff_seconds: 10 },
      null,
    )
    const retrying = runUntil(store, tick, job.job_id, ['retrying'])
    expect(retrying.attempt).toBe(2)
    expect(retrying.next_attempt_at).not.toBeNull()
    const done = runUntil(store, tick, job.job_id, ['completed'])
    expect(done.attempt).toBe(3)
    const reasons = store.getJob(job.job_id).transitions.filter((t) => t.to_state === 'retrying').map((t) => t.reason)
    expect(reasons).toEqual(['retry 2/3 in 1s', 'retry 3/3 in 2s'])
    expect(store.getJob(job.job_id).executions.map((e) => e.status)).toEqual(['completed', 'failed', 'failed'])
  })

  it('dead-letters exhausted and permanent failures; manual retry gets a fresh budget', () => {
    const { store, tick } = setup()
    const exhausted = store.createJob({ name: 'x', job_type: 'test.fail', payload: { message: 'disk full' }, priority: 'normal', max_attempts: 2, initial_backoff_seconds: 1 }, null).job
    const permanent = store.createJob({ name: 'p', job_type: 'test.fail', payload: { message: 'bad config', permanent: true }, priority: 'normal' }, null).job

    const dead = runUntil(store, tick, exhausted.job_id, ['failed'])
    expect(dead.attempt).toBe(2)
    expect(dead.dead_lettered_at).not.toBeNull()
    expect(dead.last_error).toBe('RuntimeError: disk full [attempts exhausted (2)]')
    const perm = runUntil(store, tick, permanent.job_id, ['failed'])
    expect(perm.attempt).toBe(1)
    expect(perm.last_error).toBe('PermanentTaskError: bad config [permanent error]')

    const retried = store.retryJob(exhausted.job_id)
    expect(retried).toMatchObject({ state: 'retrying', attempt: 3, max_attempts: 4, dead_lettered_at: null, can_retry: false })
    expect(() => store.retryJob(exhausted.job_id)).toThrow(DemoError)
  })

  it('cancels running work and refuses to cancel finished work', () => {
    const { store, tick } = setup()
    const { job } = store.createJob({ name: 'long', job_type: 'test.sleep', payload: { seconds: 30 }, priority: 'normal' }, null)
    runUntil(store, tick, job.job_id, ['running'])
    const cancelled = store.cancelJob(job.job_id, 'operator stop')
    expect(cancelled.state).toBe('cancelled')
    expect(store.getJob(job.job_id).executions[0]).toMatchObject({ status: 'cancelled', error: 'operator stop' })
    expect(() => store.cancelJob(job.job_id)).toThrowError(expect.objectContaining({ status: 409, code: 'not_cancellable' }))
  })

  it('never runs more jobs than healthy capacity, and never on the unhealthy worker', () => {
    const { store, tick } = setup()
    const unhealthy = store.listWorkers().find((w) => w.status === 'unhealthy')!.worker_id
    const ids = Array.from({ length: 25 }, (_, i) =>
      store.createJob({ name: `load-${i}`, job_type: 'test.sleep', payload: { seconds: 3 }, priority: 'low' }, null).job.job_id,
    )
    for (let t = 0; t < 30_000; t += 250) {
      tick(250)
      const workers = store.listWorkers()
      for (const w of workers) expect(w.running_jobs).toBeLessThanOrEqual(w.capacity_slots)
      expect(workers.find((w) => w.worker_id === unhealthy)!.running_jobs).toBe(0)
    }
    expect(ids.every((id) => store.getJob(id).job.state === 'completed')).toBe(true)
  })

  it('validates input and honours idempotency keys', () => {
    const { store } = setup()
    expect(() => store.createJob({ name: 'n', job_type: 'shell.exec', payload: {}, priority: 'normal' }, null)).toThrowError(
      expect.objectContaining({ status: 422, code: 'job_rejected' }),
    )
    expect(() => store.createJob({ name: '', job_type: 'echo', payload: {}, priority: 'normal' }, null)).toThrow(DemoError)
    const first = store.createJob({ name: 'once', job_type: 'echo', payload: {}, priority: 'normal' }, 'key-1')
    const again = store.createJob({ name: 'once', job_type: 'echo', payload: {}, priority: 'normal' }, 'key-1')
    expect(first.created).toBe(true)
    expect(again).toEqual({ job: expect.objectContaining({ job_id: first.job.job_id }), created: false })
  })
})

describe('DemoStore dead-letter tooling', () => {
  it('filters by error and bulk-retries only matching dead-lettered jobs', () => {
    const { store, tick } = setup()
    const make = (name: string, message: string) =>
      store.createJob({ name, job_type: 'test.fail', payload: { message }, priority: 'normal', max_attempts: 1 }, null).job.job_id
    const timeouts = [make('a', 'upstream TIMEOUT'), make('b', 'upstream timeout')]
    const other = make('c', 'quota 100% used')
    for (const id of [...timeouts, other]) runUntil(store, tick, id, ['failed'])

    const listed = store.listJobs({ deadLettered: true, error: 'timeout', page: 1, pageSize: 100 })
    expect(listed.items.map((j) => j.job_id).sort()).toEqual([...timeouts].sort())
    // Filters are literal substrings, not patterns.
    expect(store.listJobs({ error: '100%', page: 1, pageSize: 100 }).items.map((j) => j.job_id)).toEqual([other])

    const result = store.bulkRetry({ dead_lettered: true, error: 'timeout', limit: 100 })
    expect(result.retried.map((j) => j.job_id).sort()).toEqual([...timeouts].sort())
    expect(store.getJob(other).job.state).toBe('failed')

    const explicit = store.bulkRetry({ job_ids: [other, timeouts[0], 'missing'] })
    expect(explicit.retried.map((j) => j.job_id)).toEqual([other])
    expect(explicit.skipped.map((s) => s.code)).toEqual(['not_retryable', 'not_found'])
    expect(() => store.bulkRetry({ job_ids: [] })).toThrow(DemoError)
  })
})

describe('DemoStore over time', () => {
  it('keeps producing background traffic, and skips the gap after a long sleep', () => {
    const { store, tick } = setup()
    const before = allJobs(store).length
    tick(60_000)
    const after = allJobs(store).length
    expect(after - before).toBeGreaterThanOrEqual(4)
    tick(6 * 3600e3) // tab asleep for six hours
    expect(allJobs(store).length - after).toBeLessThan(10)
  })

  it('reset restores the seeded state', () => {
    const { store } = setup()
    const seeded = allJobs(store).length
    store.createJob({ name: 'extra', job_type: 'echo', payload: {}, priority: 'normal' }, null)
    store.reset()
    expect(allJobs(store)).toHaveLength(seeded)
  })
})
