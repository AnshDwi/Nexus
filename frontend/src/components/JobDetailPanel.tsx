import { Ban, RotateCcw } from 'lucide-react'
import { ApiError } from '../lib/api'
import { formatDateTime, formatDuration, shortId } from '../lib/format'
import { useCancelJob, useJob, useRetryJob } from '../lib/queries'
import { ExecutionBadge, PriorityBadge, StateBadge } from './Badges'
import { ErrorState, LoadingState } from './States'
import { useToast } from './Toast'

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-xs uppercase tracking-wide text-ink-400">{label}</dt>
      <dd className="mt-1 break-words text-sm text-ink-100">{children}</dd>
    </div>
  )
}

export function JobDetailPanel({ jobId }: { jobId: string }) {
  const { data, isLoading, error, refetch } = useJob(jobId)
  const cancel = useCancelJob()
  const retry = useRetryJob()
  const { notify } = useToast()

  if (isLoading) return <LoadingState rows={6} label="Loading job" />
  if (error || !data) return <ErrorState error={error} onRetry={() => void refetch()} />
  const { job, transitions, executions } = data

  const run = async (action: 'cancel' | 'retry') => {
    try {
      if (action === 'cancel') await cancel.mutateAsync({ jobId: job.job_id })
      else await retry.mutateAsync(job.job_id)
      notify({ tone: 'success', title: action === 'cancel' ? 'Job cancelled' : 'Retry scheduled' })
    } catch (e) {
      notify({ tone: 'error', title: `Could not ${action} job`, description: e instanceof ApiError ? e.message : undefined })
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-2">
        <StateBadge state={job.state} />
        <PriorityBadge priority={job.priority} />
        <span className="font-mono text-xs text-ink-400">{job.job_id}</span>
        <div className="ml-auto flex gap-2">
          {job.can_retry && (
            <button type="button" className="btn-primary" disabled={retry.isPending} onClick={() => void run('retry')}>
              <RotateCcw className="h-4 w-4" aria-hidden /> Retry
            </button>
          )}
          {job.can_cancel && (
            <button type="button" className="btn-danger" disabled={cancel.isPending} onClick={() => void run('cancel')}>
              <Ban className="h-4 w-4" aria-hidden /> Cancel
            </button>
          )}
        </div>
      </div>

      {job.last_error && (
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">
          <span className="font-medium">Last error:</span> {job.last_error}
        </div>
      )}

      <dl className="grid grid-cols-2 gap-4 sm:grid-cols-3">
        <Field label="Type"><span className="font-mono">{job.job_type}</span></Field>
        <Field label="Attempt">{job.attempt} / {job.max_attempts}</Field>
        <Field label="Timeout">{job.timeout_seconds}s</Field>
        <Field label="Created">{formatDateTime(job.created_at)}</Field>
        <Field label="Started">{formatDateTime(job.started_at)}</Field>
        <Field label="Finished">{formatDateTime(job.finished_at)}</Field>
        <Field label="Worker"><span className="font-mono">{shortId(job.worker_id)}</span></Field>
        <Field label="Next attempt">{formatDateTime(job.next_attempt_at)}</Field>
        <Field label="Correlation ID"><span className="font-mono text-xs">{job.correlation_id ?? '—'}</span></Field>
      </dl>

      <div className="grid gap-4 lg:grid-cols-2">
        <div>
          <h3 className="label">Payload</h3>
          <pre className="max-h-48 overflow-auto rounded-lg bg-ink-950 p-3 font-mono text-xs text-ink-200">{JSON.stringify(job.payload, null, 2)}</pre>
        </div>
        <div>
          <h3 className="label">Result</h3>
          <pre className="max-h-48 overflow-auto rounded-lg bg-ink-950 p-3 font-mono text-xs text-ink-200">{job.result === null || job.result === undefined ? '—' : JSON.stringify(job.result, null, 2)}</pre>
        </div>
      </div>

      <section>
        <h3 className="label">Execution attempts</h3>
        {executions.length === 0 ? (
          <p className="text-sm text-ink-400">No attempt has started yet.</p>
        ) : (
          <div className="overflow-x-auto rounded-lg border border-ink-700">
            <table className="w-full min-w-[520px]">
              <thead className="bg-ink-900/60">
                <tr><th className="th">#</th><th className="th">Status</th><th className="th">Worker</th><th className="th">Duration</th><th className="th">Error</th></tr>
              </thead>
              <tbody className="divide-y divide-ink-700">
                {executions.map((e) => (
                  <tr key={e.execution_id}>
                    <td className="td tabular-nums">{e.attempt}</td>
                    <td className="td"><ExecutionBadge status={e.status} /></td>
                    <td className="td font-mono text-xs">{shortId(e.worker_id)}</td>
                    <td className="td tabular-nums">{formatDuration(e.duration_ms)}</td>
                    <td className="td max-w-xs truncate text-rose-200" title={e.error ?? undefined}>{e.error ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section>
        <h3 className="label">State history</h3>
        <ol className="relative space-y-3 border-l border-ink-700 pl-4">
          {transitions.map((t, i) => (
            <li key={i} className="relative">
              <span className="absolute -left-[21px] top-1.5 h-2.5 w-2.5 rounded-full border-2 border-ink-850 bg-accent-500" aria-hidden />
              <div className="flex flex-wrap items-center gap-2 text-sm">
                <StateBadge state={t.from_state} /> <span className="text-ink-500">→</span> <StateBadge state={t.to_state} />
                <span className="text-xs text-ink-400">attempt {t.attempt} · {formatDateTime(t.occurred_at)}</span>
              </div>
              {t.reason && <p className="mt-1 text-xs text-ink-300">{t.reason}</p>}
            </li>
          ))}
        </ol>
      </section>
    </div>
  )
}
