import type {
  CreateJobInput,
  Execution,
  Job,
  JobDetail,
  JobFilters,
  Paged,
  QueueSnapshot,
  Summary,
  SystemInfo,
  Worker,
} from './types'

export interface Connection {
  baseUrl: string
  apiKey: string
}

/** Error returned by the NEXUS API, normalised from FastAPI's `detail`. */
export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly details: unknown
  readonly requestId: string | null

  constructor(status: number, code: string, message: string, details: unknown, requestId: string | null) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.details = details
    this.requestId = requestId
  }
}

function describe(status: number, body: unknown): { code: string; message: string; details: unknown } {
  const detail = (body as { detail?: unknown } | null)?.detail
  if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
    const d = detail as { code?: string; message?: string; details?: unknown }
    return { code: d.code ?? `http_${status}`, message: d.message ?? 'Request failed', details: d.details }
  }
  if (Array.isArray(detail)) {
    // FastAPI request validation errors
    const first = detail[0] as { loc?: unknown[]; msg?: string } | undefined
    const field = first?.loc?.slice(1).join('.') ?? 'request'
    return { code: 'validation_error', message: `${field}: ${first?.msg ?? 'invalid'}`, details: detail }
  }
  if (typeof detail === 'string') return { code: `http_${status}`, message: detail, details: undefined }
  const fallback: Record<number, string> = {
    401: 'Invalid or missing API key',
    404: 'Not found',
    429: 'Too many requests — slow down',
  }
  return { code: `http_${status}`, message: fallback[status] ?? `Request failed (${status})`, details: undefined }
}

export class NexusClient {
  private readonly connection: Connection
  private readonly fetchImpl: typeof fetch

  constructor(connection: Connection, fetchImpl: typeof fetch = (...args) => fetch(...args)) {
    this.connection = connection
    this.fetchImpl = fetchImpl
  }

  private url(path: string, query?: URLSearchParams): string {
    const base = this.connection.baseUrl.replace(/\/+$/, '')
    const qs = query && query.toString() ? `?${query.toString()}` : ''
    return `${base}/api/v1${path}${qs}`
  }

  async request<T>(
    method: 'GET' | 'POST',
    path: string,
    options: { query?: URLSearchParams; body?: unknown; headers?: Record<string, string> } = {},
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: 'application/json', ...options.headers }
    if (this.connection.apiKey) headers.Authorization = `Bearer ${this.connection.apiKey}`
    if (options.body !== undefined) headers['Content-Type'] = 'application/json'
    let response: Response
    try {
      response = await this.fetchImpl(this.url(path, options.query), {
        method,
        headers,
        body: options.body === undefined ? undefined : JSON.stringify(options.body),
      })
    } catch {
      throw new ApiError(0, 'network_error', 'Cannot reach the NEXUS API', undefined, null)
    }
    const text = await response.text()
    let body: unknown = null
    if (text) {
      try {
        body = JSON.parse(text)
      } catch {
        body = null
      }
    }
    if (!response.ok) {
      const { code, message, details } = describe(response.status, body)
      throw new ApiError(response.status, code, message, details, response.headers.get('X-Request-ID'))
    }
    return body as T
  }

  health() {
    return this.request<{ status: string }>('GET', '/health')
  }
  ready() {
    return this.request<Record<string, string>>('GET', '/ready').catch((e: unknown) => {
      // 503 still carries the per-dependency report.
      if (e instanceof ApiError && e.status === 503) return { status: 'degraded' } as Record<string, string>
      throw e
    })
  }
  system() {
    return this.request<SystemInfo>('GET', '/system')
  }
  summary(hours = 24) {
    return this.request<Summary>('GET', '/metrics/summary', { query: new URLSearchParams({ hours: String(hours) }) })
  }
  listJobs(filters: JobFilters) {
    const query = new URLSearchParams({ page: String(filters.page), page_size: String(filters.pageSize) })
    filters.status.forEach((s) => query.append('status', s))
    filters.priority.forEach((p) => query.append('priority', p))
    if (filters.q.trim()) query.set('q', filters.q.trim())
    if (filters.deadLettered) query.set('dead_lettered', 'true')
    return this.request<Paged<Job>>('GET', '/jobs', { query })
  }
  getJob(jobId: string) {
    return this.request<JobDetail>('GET', `/jobs/${encodeURIComponent(jobId)}`)
  }
  createJob(input: CreateJobInput, idempotencyKey: string) {
    return this.request<Job>('POST', '/jobs', { body: input, headers: { 'Idempotency-Key': idempotencyKey } })
  }
  cancelJob(jobId: string, reason?: string) {
    return this.request<Job>('POST', `/jobs/${encodeURIComponent(jobId)}/cancel`, {
      body: reason ? { reason } : undefined,
    })
  }
  retryJob(jobId: string) {
    return this.request<Job>('POST', `/jobs/${encodeURIComponent(jobId)}/retry`)
  }
  workers() {
    return this.request<Worker[]>('GET', '/workers')
  }
  queue() {
    return this.request<QueueSnapshot>('GET', '/queue')
  }
  executions(params: { page: number; pageSize: number; status?: string; jobId?: string }) {
    const query = new URLSearchParams({ page: String(params.page), page_size: String(params.pageSize) })
    if (params.status) query.set('status', params.status)
    if (params.jobId) query.set('job_id', params.jobId)
    return this.request<Paged<Execution>>('GET', '/executions', { query })
  }
}
