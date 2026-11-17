import { http, HttpResponse } from 'msw'
import { describe, expect, it } from 'vitest'
import { API, API_KEY, db, server } from '../test/server'
import { ApiError, NexusClient } from './api'

const client = new NexusClient({ baseUrl: 'http://nexus.test/', apiKey: API_KEY })

describe('NexusClient (API integration)', () => {
  it('sends the bearer key and builds filter query strings', async () => {
    await client.listJobs({ status: ['failed', 'retrying'], priority: ['critical'], q: ' report ', page: 2, pageSize: 10 })
    const request = db.requests.at(-1)!
    const url = new URL(request.url)
    expect(request.headers.get('Authorization')).toBe(`Bearer ${API_KEY}`)
    expect(url.pathname).toBe('/api/v1/jobs')
    expect(url.searchParams.getAll('status')).toEqual(['failed', 'retrying'])
    expect(url.searchParams.get('priority')).toBe('critical')
    expect(url.searchParams.get('q')).toBe('report')
    expect(url.searchParams.get('page')).toBe('2')
  })

  it('sends an Idempotency-Key with job creation', async () => {
    const job = await client.createJob({ name: 'deploy', job_type: 'echo', payload: {}, priority: 'high' }, 'key-1')
    expect(job.name).toBe('deploy')
    const request = db.requests.at(-1)!
    expect(request.headers.get('Idempotency-Key')).toBe('key-1')
    expect(request.headers.get('Content-Type')).toBe('application/json')
  })

  it('maps structured API errors', async () => {
    const error = await client
      .createJob({ name: 'x', job_type: 'unknown', payload: {}, priority: 'low' }, 'k')
      .catch((e: unknown) => e)
    expect(error).toBeInstanceOf(ApiError)
    expect((error as ApiError).status).toBe(422)
    expect((error as ApiError).code).toBe('job_rejected')
  })

  it('maps FastAPI validation errors to a readable message', async () => {
    server.use(
      http.get(`${API}/jobs`, () =>
        HttpResponse.json({ detail: [{ loc: ['query', 'page'], msg: 'Input should be greater than 0' }] }, { status: 422 }),
      ),
    )
    const error = (await client.listJobs({ status: [], priority: [], q: '', page: 0, pageSize: 1 }).catch((e: unknown) => e)) as ApiError
    expect(error.message).toBe('page: Input should be greater than 0')
  })

  it('reports 401 for a wrong key and network failures distinctly', async () => {
    const wrong = new NexusClient({ baseUrl: 'http://nexus.test', apiKey: 'nope' })
    await expect(wrong.system()).rejects.toMatchObject({ status: 401, message: 'invalid or missing credentials' })
    server.use(http.get(`${API}/health`, () => HttpResponse.error()))
    await expect(client.health()).rejects.toMatchObject({ code: 'network_error' })
  })

  it('treats a 503 readiness response as degraded rather than an exception', async () => {
    server.use(http.get(`${API}/ready`, () => HttpResponse.json({ status: 'degraded' }, { status: 503 })))
    await expect(client.ready()).resolves.toEqual({ status: 'degraded' })
  })
})
