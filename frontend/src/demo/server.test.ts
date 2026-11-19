import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, NexusClient } from '../lib/api'
import { createDemoFetch } from './server'
import { DemoStore } from './store'

function client() {
  const store = new DemoStore({ seed: 3 })
  return { store, api: new NexusClient({ baseUrl: '', apiKey: 'demo' }, createDemoFetch(store)) }
}

describe('demo fetch: the NEXUS API served from the simulator', () => {
  afterEach(() => vi.restoreAllMocks())

  it('never touches the network', async () => {
    const network = vi.spyOn(globalThis, 'fetch')
    const { api } = client()
    await api.summary()
    await api.listJobs({ status: [], priority: [], q: '', page: 1, pageSize: 20 })
    await api.workers()
    await api.queue()
    await api.executions({ page: 1, pageSize: 10 })
    await api.system()
    await api.ready()
    expect(network).not.toHaveBeenCalled()
  })

  it('answers every endpoint the dashboard uses with the API shapes', async () => {
    const { api } = client()
    const summary = await api.summary()
    expect(summary.total_jobs).toBeGreaterThan(0)
    expect(summary.broker.available).toBe(true)

    const page = await api.listJobs({ status: ['failed'], priority: [], q: '', deadLettered: true, page: 1, pageSize: 5 })
    expect(page.items.length).toBeGreaterThan(0)
    expect(page.items.every((j) => j.state === 'failed' && j.dead_lettered_at)).toBe(true)
    expect(page.pages).toBe(Math.max(Math.ceil(page.total / 5), 1))

    const detail = await api.getJob(page.items[0].job_id)
    expect(detail.job.job_id).toBe(page.items[0].job_id)
    expect(detail.transitions.length).toBeGreaterThan(0)

    expect((await api.workers()).length).toBe(4)
    expect((await api.queue()).broker.consumers).toBe(3)
    expect((await api.executions({ page: 1, pageSize: 10, status: 'failed' })).items.every((e) => e.status === 'failed')).toBe(true)
    expect((await api.system()).environment).toBe('demo')
    expect(await api.ready()).toMatchObject({ status: 'ok' })
  })

  it('creates, cancels and retries through POST endpoints, honouring Idempotency-Key', async () => {
    const { api } = client()
    const job = await api.createJob({ name: 'demo-job', job_type: 'echo', payload: { a: 1 }, priority: 'high' }, 'idem-1')
    expect(job).toMatchObject({ name: 'demo-job', state: 'pending', priority: 'high' })
    expect((await api.createJob({ name: 'demo-job', job_type: 'echo', payload: {}, priority: 'high' }, 'idem-1')).job_id).toBe(job.job_id)
    expect((await api.cancelJob(job.job_id, 'changed my mind')).state).toBe('cancelled')

    const dead = (await api.listJobs({ status: [], priority: [], q: '', deadLettered: true, page: 1, pageSize: 1 })).items[0]
    expect((await api.retryJob(dead.job_id)).state).toBe('retrying')
    const bulk = await api.bulkRetry({ dead_lettered: true, limit: 100 })
    expect(bulk.requested).toBe(bulk.retried.length + bulk.skipped.length)
  })

  it('returns API-style errors that NexusClient turns into ApiError', async () => {
    const { api } = client()
    await expect(api.getJob('nope')).rejects.toMatchObject({ status: 404, code: 'not_found', message: 'job not found' })
    await expect(api.createJob({ name: 'x', job_type: 'rm -rf', payload: {}, priority: 'low' }, 'k')).rejects.toMatchObject({
      status: 422,
      code: 'job_rejected',
    })
    const done = (await api.listJobs({ status: ['completed'], priority: [], q: '', page: 1, pageSize: 1 })).items[0]
    const err = await api.retryJob(done.job_id).catch((e: unknown) => e)
    expect(err).toBeInstanceOf(ApiError)
    expect(err).toMatchObject({ status: 409, code: 'not_retryable' })
    await expect(api.bulkRetry({ job_ids: [] })).rejects.toMatchObject({ status: 422 })
  })
})
