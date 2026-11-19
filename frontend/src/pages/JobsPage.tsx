import clsx from 'clsx'
import { Ban, Plus, RotateCcw, Search, TriangleAlert } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { PriorityBadge, StateBadge } from '../components/Badges'
import { CreateJobForm } from '../components/CreateJobForm'
import { JobDetailPanel } from '../components/JobDetailPanel'
import { Modal } from '../components/Modal'
import { PageHeader } from '../components/PageHeader'
import { Pagination } from '../components/Pagination'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import { useToast } from '../components/Toast'
import { ApiError } from '../lib/api'
import { formatRelative } from '../lib/format'
import { useBulkRetry, useCancelJob, useJobs, useRetryJob } from '../lib/queries'
import { JOB_PRIORITIES, JOB_STATES, type BulkRetryInput, type Job, type JobFilters, type JobPriority, type JobState } from '../lib/types'

const BULK_LIMIT = 100

function toggle<T>(list: T[], value: T): T[] {
  return list.includes(value) ? list.filter((v) => v !== value) : [...list, value]
}

function Chip({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={clsx(
        'rounded-full border px-2.5 py-1 text-xs font-medium capitalize transition',
        active ? 'border-accent-500 bg-accent-500/15 text-accent-400' : 'border-ink-600 text-ink-300 hover:border-ink-500',
      )}
    >
      {children}
    </button>
  )
}

export function JobsPage() {
  const [params, setParams] = useSearchParams()
  const { jobId } = useParams()
  const navigate = useNavigate()
  const { notify } = useToast()
  const cancel = useCancelJob()
  const retry = useRetryJob()
  const bulk = useBulkRetry()

  const [search, setSearch] = useState(params.get('q') ?? '')
  const [errorText, setErrorText] = useState(params.get('error') ?? '')
  // Selection and the "retry all" confirmation belong to one result page:
  // they are stored with the query they were made on and ignored after it changes.
  const [selection, setSelection] = useState<{ query: string; ids: Set<string> }>(() => ({ query: '', ids: new Set() }))
  const [confirmQuery, setConfirmQuery] = useState<string | null>(null)
  const filters: JobFilters = useMemo(
    () => ({
      status: params.getAll('status') as JobState[],
      priority: params.getAll('priority') as JobPriority[],
      q: params.get('q') ?? '',
      deadLettered: params.get('dead_lettered') === '1',
      error: params.get('error') ?? '',
      page: Number(params.get('page') ?? '1') || 1,
      pageSize: 20,
    }),
    [params],
  )
  const { data, isLoading, error, refetch, isFetching } = useJobs(filters)

  // Debounce the search and error boxes into the URL.
  useEffect(() => {
    const t = setTimeout(() => {
      const wanted: [string, string][] = [['q', search], ['error', errorText]]
      if (wanted.every(([k, v]) => (params.get(k) ?? '') === v)) return
      const next = new URLSearchParams(params)
      for (const [k, v] of wanted) {
        if (v) next.set(k, v)
        else next.delete(k)
      }
      next.delete('page')
      setParams(next, { replace: true })
    }, 300)
    return () => clearTimeout(t)
  }, [search, errorText, params, setParams])

  const query = params.toString()
  const selected = selection.query === query ? selection.ids : new Set<string>()
  const setSelected = (ids: Set<string>) => setSelection({ query, ids })
  const confirmAll = confirmQuery === query
  const setConfirmAll = (on: boolean) => setConfirmQuery(on ? query : null)
  const retryable = (data?.items ?? []).filter((j) => j.can_retry)
  const chosen = retryable.filter((j) => selected.has(j.job_id))
  const allChosen = retryable.length > 0 && chosen.length === retryable.length
  const flip = (id: string) => {
    const next = new Set(selected)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    setSelected(next)
  }

  const runBulk = async (input: BulkRetryInput) => {
    try {
      const result = await bulk.mutateAsync(input)
      const n = result.retried.length
      notify({
        tone: n > 0 ? 'success' : 'error',
        title: n > 0 ? `Retrying ${n} job${n === 1 ? '' : 's'}` : 'No jobs were retried',
        description: result.skipped.length
          ? `${result.skipped.length} skipped (${result.skipped[0].message})`
          : undefined,
      })
      setSelected(new Set())
      setConfirmAll(false)
    } catch (e) {
      notify({ tone: 'error', title: 'Bulk retry failed', description: e instanceof ApiError ? e.message : undefined })
    }
  }
  const showErrorFilter = Boolean(filters.deadLettered) || filters.status.includes('failed')

  const update = (key: 'status' | 'priority', values: string[]) => {
    const next = new URLSearchParams(params)
    next.delete(key)
    values.forEach((v) => next.append(key, v))
    next.delete('page')
    setParams(next)
  }
  const setPage = (page: number) => {
    const next = new URLSearchParams(params)
    next.set('page', String(page))
    setParams(next)
  }
  const creating = params.get('create') === '1'
  const closeCreate = () => {
    const next = new URLSearchParams(params)
    next.delete('create')
    setParams(next)
  }

  const act = async (job: Job, action: 'cancel' | 'retry') => {
    try {
      if (action === 'cancel') await cancel.mutateAsync({ jobId: job.job_id })
      else await retry.mutateAsync(job.job_id)
      notify({ tone: 'success', title: action === 'cancel' ? `Cancelled ${job.name}` : `Retrying ${job.name}` })
    } catch (e) {
      notify({ tone: 'error', title: `Could not ${action} ${job.name}`, description: e instanceof ApiError ? e.message : undefined })
    }
  }

  return (
    <>
      <PageHeader
        title="Jobs"
        description="Submit work, follow it through the pipeline, cancel or retry it."
        actions={
          <button type="button" className="btn-primary" onClick={() => setParams(new URLSearchParams({ ...Object.fromEntries(params), create: '1' }))}>
            <Plus className="h-4 w-4" aria-hidden /> Create job
          </button>
        }
      />

      <div className="card mb-4 space-y-3 p-4">
        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-ink-400" aria-hidden />
          <input
            className="input pl-9"
            placeholder="Search by name, type or job ID"
            aria-label="Search jobs"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="mr-1 text-xs uppercase tracking-wide text-ink-400">Status</span>
          {JOB_STATES.map((s) => (
            <Chip key={s} active={filters.status.includes(s)} onClick={() => update('status', toggle(filters.status, s))}>{s}</Chip>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="mr-1 text-xs uppercase tracking-wide text-ink-400">Priority</span>
          {JOB_PRIORITIES.map((p) => (
            <Chip key={p} active={filters.priority.includes(p)} onClick={() => update('priority', toggle(filters.priority, p))}>{p}</Chip>
          ))}
          <span className="mx-1 h-4 w-px bg-ink-700" aria-hidden />
          <Chip
            active={Boolean(filters.deadLettered)}
            onClick={() => {
              const next = new URLSearchParams(params)
              if (filters.deadLettered) next.delete('dead_lettered')
              else next.set('dead_lettered', '1')
              next.delete('page')
              setParams(next)
            }}
          >
            Dead-lettered
          </Chip>
          {(filters.status.length > 0 || filters.priority.length > 0 || filters.q || filters.deadLettered || filters.error) && (
            <button type="button" className="btn-ghost ml-auto px-2 py-1 text-xs" onClick={() => { setSearch(''); setErrorText(''); setParams(new URLSearchParams()) }}>
              Clear filters
            </button>
          )}
        </div>
        {showErrorFilter && (
          <div className="relative">
            <TriangleAlert className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-ink-400" aria-hidden />
            <input
              className="input pl-9"
              placeholder="Last error contains… (e.g. timeout, 403)"
              aria-label="Filter by last error"
              value={errorText}
              onChange={(e) => setErrorText(e.target.value)}
            />
          </div>
        )}
      </div>

      {data && (chosen.length > 0 || (filters.deadLettered && data.total > 0)) && (
        <div className="card mb-4 flex flex-wrap items-center gap-2 p-3" role="toolbar" aria-label="Bulk actions">
          {chosen.length > 0 && (
            <>
              <span className="text-sm text-ink-200">{chosen.length} selected</span>
              <button
                type="button"
                className="btn-primary px-3 py-1.5 text-xs"
                disabled={bulk.isPending}
                onClick={() => void runBulk({ job_ids: chosen.map((j) => j.job_id) })}
              >
                <RotateCcw className="h-3.5 w-3.5" aria-hidden /> Retry selected
              </button>
              <button type="button" className="btn-ghost px-2 py-1 text-xs" onClick={() => setSelected(new Set())}>
                Clear selection
              </button>
            </>
          )}
          {filters.deadLettered && data.total > 0 && (
            <div className="ml-auto flex items-center gap-2">
              {confirmAll ? (
                <>
                  <span className="text-sm text-amber-300">
                    Retry {Math.min(data.total, BULK_LIMIT)} dead-lettered job{data.total === 1 ? '' : 's'}
                    {filters.error ? ` matching “${filters.error}”` : ''}?
                  </span>
                  <button
                    type="button"
                    className="btn-primary px-3 py-1.5 text-xs"
                    disabled={bulk.isPending}
                    onClick={() =>
                      void runBulk({
                        dead_lettered: true,
                        ...(filters.error ? { error: filters.error } : {}),
                        ...(filters.q ? { q: filters.q } : {}),
                        limit: BULK_LIMIT,
                      })
                    }
                  >
                    Confirm retry
                  </button>
                  <button type="button" className="btn-ghost px-2 py-1 text-xs" onClick={() => setConfirmAll(false)}>
                    Keep them
                  </button>
                </>
              ) : (
                <button type="button" className="btn-ghost px-3 py-1.5 text-xs" onClick={() => setConfirmAll(true)}>
                  <RotateCcw className="h-3.5 w-3.5" aria-hidden /> Retry all matching ({Math.min(data.total, BULK_LIMIT)}
                  {data.total > BULK_LIMIT ? ` of ${data.total}` : ''})
                </button>
              )}
            </div>
          )}
        </div>
      )}

      <div className="card overflow-hidden">
        {isLoading ? (
          <LoadingState rows={8} label="Loading jobs" />
        ) : error ? (
          <ErrorState error={error} onRetry={() => void refetch()} />
        ) : !data || data.items.length === 0 ? (
          <EmptyState
            title={filters.status.length || filters.priority.length || filters.q || filters.deadLettered || filters.error ? 'No jobs match these filters' : 'No jobs yet'}
            description="Create a job to dispatch it to a worker."
          />
        ) : (
          <>
            <div className={clsx('overflow-x-auto', isFetching && 'opacity-90')}>
              <table className="w-full min-w-[760px]">
                <thead className="border-b border-ink-700 bg-ink-900/50">
                  <tr>
                    <th className="th w-10">
                      <input
                        type="checkbox"
                        className="h-4 w-4 accent-accent-500"
                        aria-label="Select all retryable jobs on this page"
                        disabled={retryable.length === 0}
                        checked={allChosen}
                        onChange={() => setSelected(allChosen ? new Set() : new Set(retryable.map((j) => j.job_id)))}
                      />
                    </th>
                    <th className="th">Job</th><th className="th">Status</th><th className="th">Priority</th>
                    <th className="th">Attempt</th><th className="th">Created</th><th className="th text-right">Actions</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-ink-700">
                  {data.items.map((job) => (
                    <tr key={job.job_id} className="cursor-pointer hover:bg-ink-800/50" onClick={() => navigate(`/jobs/${job.job_id}?${params.toString()}`)}>
                      <td className="td" onClick={(e) => e.stopPropagation()}>
                        {job.can_retry && (
                          <input
                            type="checkbox"
                            className="h-4 w-4 accent-accent-500"
                            aria-label={`Select ${job.name}`}
                            checked={selected.has(job.job_id)}
                            onChange={() => flip(job.job_id)}
                          />
                        )}
                      </td>
                      <td className="td">
                        <div className="font-medium text-ink-100">{job.name}</div>
                        <div className="font-mono text-xs text-ink-400">{job.job_type} · {job.job_id.slice(0, 8)}</div>
                        {job.state === 'failed' && job.last_error && (
                          <div className="mt-0.5 max-w-md truncate text-xs text-rose-300/80" title={job.last_error}>
                            {job.last_error}
                          </div>
                        )}
                      </td>
                      <td className="td">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <StateBadge state={job.state} />
                          {job.dead_lettered_at && (
                            <span className="rounded-full bg-fuchsia-500/10 px-2 py-0.5 text-[11px] font-medium text-fuchsia-300 ring-1 ring-inset ring-fuchsia-500/30">
                              dead-lettered
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="td"><PriorityBadge priority={job.priority} /></td>
                      <td className="td tabular-nums">{job.attempt}/{job.max_attempts}</td>
                      <td className="td text-ink-300">{formatRelative(job.created_at)}</td>
                      <td className="td">
                        <div className="flex justify-end gap-1" onClick={(e) => e.stopPropagation()}>
                          {job.can_retry && (
                            <button type="button" className="btn-ghost px-2 py-1 text-xs" onClick={() => void act(job, 'retry')} aria-label={`Retry ${job.name}`}>
                              <RotateCcw className="h-3.5 w-3.5" aria-hidden /> Retry
                            </button>
                          )}
                          {job.can_cancel && (
                            <button type="button" className="btn-ghost px-2 py-1 text-xs text-rose-300" onClick={() => void act(job, 'cancel')} aria-label={`Cancel ${job.name}`}>
                              <Ban className="h-3.5 w-3.5" aria-hidden /> Cancel
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pagination page={data.page} pages={data.pages} total={data.total} onPage={setPage} />
          </>
        )}
      </div>

      <Modal open={creating} title="Create job" onClose={closeCreate}>
        <CreateJobForm
          onCancel={closeCreate}
          onCreated={(job) => {
            notify({ tone: 'success', title: 'Job submitted', description: `${job.name} is ${job.state}` })
            navigate(`/jobs/${job.job_id}`)
          }}
        />
      </Modal>
      <Modal open={Boolean(jobId)} title="Job details" wide onClose={() => navigate(`/jobs?${new URLSearchParams([...params].filter(([k]) => k !== 'create')).toString()}`)}>
        {jobId && <JobDetailPanel jobId={jobId} />}
      </Modal>
    </>
  )
}
