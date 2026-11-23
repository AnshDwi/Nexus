import { useState } from 'react'
import { Link } from 'react-router-dom'
import { ExecutionBadge } from '../components/Badges'
import { PageHeader } from '../components/PageHeader'
import { Pagination } from '../components/Pagination'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import { formatDateTime, formatDuration, shortId } from '../lib/format'
import { useExecutions } from '../lib/queries'

const STATUSES = ['', 'running', 'completed', 'failed', 'timed_out', 'lost', 'cancelled']

export function ExecutionsPage() {
  const [page, setPage] = useState(1)
  const [status, setStatus] = useState('')
  const { data, isLoading, error, refetch } = useExecutions({ page, pageSize: 25, status: status || undefined })
  return (
    <>
      <PageHeader
        title="Execution History"
        description="Every attempt every worker made: timing, outcome, failure reasons and retries."
        actions={
          <select aria-label="Filter by outcome" className="input w-44" value={status} onChange={(e) => { setStatus(e.target.value); setPage(1) }}>
            {STATUSES.map((s) => <option key={s} value={s}>{s ? s.replace('_', ' ') : 'All outcomes'}</option>)}
          </select>
        }
      />
      <div className="card overflow-hidden">
        {isLoading ? (
          <LoadingState rows={8} label="Loading executions" />
        ) : error ? (
          <ErrorState error={error} onRetry={() => void refetch()} />
        ) : !data || data.items.length === 0 ? (
          <EmptyState title="No executions recorded" description="Attempts appear here once a worker starts a job." />
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[900px]">
                <thead className="border-b border-ink-700 bg-ink-900/50">
                  <tr><th className="th">Job</th><th className="th">Attempt</th><th className="th">Outcome</th><th className="th">Started</th><th className="th">Ended</th><th className="th">Duration</th><th className="th">Worker</th><th className="th">Failure reason</th></tr>
                </thead>
                <tbody className="divide-y divide-ink-700">
                  {data.items.map((e) => (
                    <tr key={e.execution_id}>
                      <td className="td"><Link to={`/jobs/${e.job_id}`} className="font-medium text-ink-100 hover:text-accent-400">{e.job_name}</Link></td>
                      <td className="td tabular-nums">#{e.attempt}{e.attempt > 1 && <span className="ml-1 text-xs text-amber-300">retry</span>}</td>
                      <td className="td"><ExecutionBadge status={e.status} /></td>
                      <td className="td text-ink-300">{formatDateTime(e.started_at)}</td>
                      <td className="td text-ink-300">{formatDateTime(e.finished_at)}</td>
                      <td className="td tabular-nums">{formatDuration(e.duration_ms)}</td>
                      <td className="td font-mono text-xs">{shortId(e.worker_id)}</td>
                      <td className="td max-w-xs truncate text-rose-200" title={e.error ?? undefined}>{e.error ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pagination page={data.page} pages={data.pages} total={data.total} onPage={setPage} />
          </>
        )}
      </div>
    </>
  )
}
