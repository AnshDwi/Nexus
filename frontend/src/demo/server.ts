/**
 * A `fetch` implementation that answers NEXUS API requests from the demo
 * store. `NexusClient` is given this instead of the browser's fetch, so every
 * page, hook and error path runs exactly as it does against the real API;
 * only the transport is swapped. No request ever leaves the browser.
 */
import type { BulkRetryInput, CreateJobInput } from '../lib/types'
import { DemoError, type DemoStore } from './store'

const API_PREFIX = '/api/v1'
const LATENCY_MS = 60 // keeps loading states honest without feeling slow

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', 'X-Request-ID': 'demo' },
  })
}

function error(status: number, code: string, message: string): Response {
  return json({ detail: { code, message } }, status)
}

async function readBody<T>(init?: RequestInit): Promise<T | undefined> {
  if (!init?.body) return undefined
  return JSON.parse(String(init.body)) as T
}

function page(q: URLSearchParams, fallbackSize: number): { page: number; pageSize: number } {
  return {
    page: Math.max(Number(q.get('page') ?? 1) || 1, 1),
    pageSize: Math.min(Math.max(Number(q.get('page_size') ?? fallbackSize) || fallbackSize, 1), 100),
  }
}

export function createDemoFetch(store: DemoStore): typeof fetch {
  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.href : input.url, 'http://demo.local')
    const method = (init?.method ?? 'GET').toUpperCase()
    await new Promise((resolve) => setTimeout(resolve, LATENCY_MS))
    if (!url.pathname.startsWith(API_PREFIX)) return error(404, 'not_found', 'Not found')
    const path = url.pathname.slice(API_PREFIX.length)
    const q = url.searchParams
    const headers = new Headers(init?.headers)

    try {
      if (method === 'GET') {
        if (path === '/health') return json({ status: 'ok' })
        if (path === '/ready') return json({ status: 'ok', postgres: 'simulated', rabbitmq: 'simulated', redis: 'simulated' })
        if (path === '/system') return json(store.system())
        if (path === '/metrics/summary') return json(store.summary(Number(q.get('hours') ?? 24) || 24))
        if (path === '/workers') return json(store.listWorkers())
        if (path === '/queue') return json(store.queue(Number(q.get('limit') ?? 100) || 100))
        if (path === '/executions') {
          return json(
            store.listExecutions({ status: q.get('status') ?? undefined, jobId: q.get('job_id') ?? undefined, ...page(q, 25) }),
          )
        }
        if (path === '/jobs') {
          const dead = q.get('dead_lettered')
          return json(
            store.listJobs({
              status: q.getAll('status'),
              priority: q.getAll('priority'),
              q: q.get('q') ?? undefined,
              deadLettered: dead === null ? null : dead === 'true',
              error: q.get('error') ?? undefined,
              ...page(q, 25),
            }),
          )
        }
        const detail = path.match(/^\/jobs\/([^/]+)$/)
        if (detail) return json(store.getJob(decodeURIComponent(detail[1])))
      }

      if (method === 'POST') {
        if (path === '/jobs') {
          const body = await readBody<CreateJobInput>(init)
          if (!body) return error(422, 'validation_error', 'body: required')
          const { job, created } = store.createJob(body, headers.get('Idempotency-Key'))
          return json(job, created ? 201 : 200)
        }
        if (path === '/jobs/bulk-retry') {
          const body = await readBody<BulkRetryInput>(init)
          if (!body) return error(422, 'validation_error', 'body: required')
          return json(store.bulkRetry(body))
        }
        const action = path.match(/^\/jobs\/([^/]+)\/(cancel|retry)$/)
        if (action) {
          const id = decodeURIComponent(action[1])
          if (action[2] === 'retry') return json(store.retryJob(id))
          const body = await readBody<{ reason?: string }>(init)
          return json(store.cancelJob(id, body?.reason))
        }
      }
      return error(404, 'not_found', 'Not found')
    } catch (e) {
      if (e instanceof DemoError) return error(e.status, e.code, e.message)
      return error(500, 'internal_error', 'The demo simulator hit an unexpected error')
    }
  }
}
