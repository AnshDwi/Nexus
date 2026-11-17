import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'
import type { Job } from '../lib/types'
import { makeExecution, makeJob, summary, system, worker } from './fixtures'

export const API = 'http://nexus.test/api/v1'
export const API_KEY = 'test-key'

/** A tiny in-memory NEXUS: enough behaviour to exercise the dashboard. */
export const db: { jobs: Job[]; requests: Request[] } = { jobs: [], requests: [] }

function authorized(request: Request) {
  return request.headers.get('Authorization') === `Bearer ${API_KEY}`
}
const unauthorized = () => HttpResponse.json({ detail: 'invalid or missing credentials' }, { status: 401 })

export const handlers = [
  http.all(`${API}/*`, ({ request }) => {
    db.requests.push(request.clone())
    return undefined // fall through to the specific handlers
  }),
  http.get(`${API}/health`, () => HttpResponse.json({ status: 'ok' })),
  http.get(`${API}/ready`, () => HttpResponse.json({ status: 'ok', postgres: 'ok' })),
  http.get(`${API}/system`, ({ request }) => (authorized(request) ? HttpResponse.json(system) : unauthorized())),
  http.get(`${API}/metrics/summary`, ({ request }) => (authorized(request) ? HttpResponse.json(summary) : unauthorized())),
  http.get(`${API}/workers`, () => HttpResponse.json([worker])),
  http.get(`${API}/queue`, () =>
    HttpResponse.json({
      pending: 1,
      queued: 1,
      retrying: 0,
      depth: 2,
      broker: summary.broker,
      unpublished_events: 0,
      items: db.jobs
        .filter((j) => ['pending', 'queued', 'retrying'].includes(j.state))
        .map((job) => ({ job, dispatch_status: job.state === 'queued' ? 'confirmed' : 'awaiting_publish', dispatch_requested_at: null })),
    }),
  ),
  http.get(`${API}/executions`, () =>
    HttpResponse.json({ items: [makeExecution()], total: 1, page: 1, page_size: 25, pages: 1 }),
  ),
  http.get(`${API}/jobs`, ({ request }) => {
    if (!authorized(request)) return unauthorized()
    const url = new URL(request.url)
    const statuses = url.searchParams.getAll('status')
    const q = url.searchParams.get('q') ?? ''
    const items = db.jobs.filter(
      (j) => (statuses.length === 0 || statuses.includes(j.state)) && (!q || j.name.includes(q)),
    )
    return HttpResponse.json({ items, total: items.length, page: 1, page_size: 20, pages: 1 })
  }),
  http.get(`${API}/jobs/:id`, ({ params }) => {
    const job = db.jobs.find((j) => j.job_id === params.id)
    if (!job) return HttpResponse.json({ detail: { code: 'not_found', message: 'job not found' } }, { status: 404 })
    return HttpResponse.json({
      job,
      transitions: [{ from_state: 'pending', to_state: 'queued', occurred_at: job.created_at, attempt: 1, reason: 'dispatch confirmed by broker' }],
      executions: [makeExecution({ job_id: job.job_id, job_name: job.name })],
    })
  }),
  http.post(`${API}/jobs`, async ({ request }) => {
    const body = (await request.json()) as { name: string; job_type: string; priority: Job['priority'] }
    if (body.job_type === 'unknown') {
      return HttpResponse.json(
        { detail: { code: 'job_rejected', message: "job_type: unknown job type 'unknown'" } },
        { status: 422 },
      )
    }
    const job = makeJob({ name: body.name, job_type: body.job_type, priority: body.priority, state: 'queued' })
    db.jobs.unshift(job)
    return HttpResponse.json(job, { status: 201 })
  }),
  http.post(`${API}/jobs/:id/cancel`, ({ params }) => {
    const job = db.jobs.find((j) => j.job_id === params.id)
    if (!job) return HttpResponse.json({ detail: { code: 'not_found', message: 'job not found' } }, { status: 404 })
    Object.assign(job, { state: 'cancelled', can_cancel: false })
    return HttpResponse.json(job)
  }),
  http.post(`${API}/jobs/:id/retry`, ({ params }) => {
    const job = db.jobs.find((j) => j.job_id === params.id)
    if (!job || job.state !== 'failed') {
      return HttpResponse.json({ detail: { code: 'not_retryable', message: 'only failed jobs can be retried' } }, { status: 409 })
    }
    Object.assign(job, { state: 'retrying', can_retry: false, can_cancel: true, attempt: job.attempt + 1 })
    return HttpResponse.json(job)
  }),
]

export const server = setupServer(...handlers)
