import { Link } from 'react-router-dom'
import { PriorityBadge, StateBadge } from '../components/Badges'
import { PageHeader } from '../components/PageHeader'
import { StatCard } from '../components/StatCard'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import { formatRelative } from '../lib/format'
import { useQueueSnapshot } from '../lib/queries'
import type { QueueItem } from '../lib/types'
import { Clock, Inbox, Layers, Radio, Skull } from 'lucide-react'

const DISPATCH: Record<QueueItem['dispatch_status'], { label: string; className: string }> = {
  confirmed: { label: 'Confirmed by broker', className: 'text-emerald-300' },
  awaiting_publish: { label: 'Awaiting publish', className: 'text-amber-300' },
  scheduled_retry: { label: 'Retry scheduled', className: 'text-sky-300' },
}

export function QueuePage() {
  const { data, isLoading, error, refetch } = useQueueSnapshot()
  if (isLoading) return <LoadingState rows={6} label="Loading queue" />
  if (error || !data) return <ErrorState error={error} onRetry={() => void refetch()} />
  return (
    <>
      <PageHeader title="Queue" description="Jobs waiting to run, ordered by priority, and their dispatch status." />
      <div className="mb-6 grid grid-cols-2 gap-4 lg:grid-cols-5">
        <StatCard label="Queue depth" value={data.depth} icon={Layers} tone="amber" />
        <StatCard label="Pending" value={data.pending} icon={Inbox} />
        <StatCard label="Queued" value={data.queued} icon={Radio} tone="blue" />
        <StatCard label="Retrying" value={data.retrying} icon={Clock} tone="amber" />
        <StatCard
          label="Dead-lettered"
          value={data.dead_lettered_jobs}
          icon={Skull}
          tone="red"
          hint={
            <>
              {data.broker.available
                ? `jobs currently dead-lettered · RabbitMQ nexus.dead-letter holds ${data.broker.dead_letter_messages ?? 0} message(s), incl. jobs since retried · ${data.broker.consumers ?? 0} consumers`
                : 'broker not reachable from API'}
              {data.dead_lettered_jobs > 0 && (
                <Link to="/jobs?dead_lettered=1" className="mt-1 block text-accent-400 hover:underline">
                  View dead-lettered jobs →
                </Link>
              )}
            </>
          }
        />
      </div>
      <div className="card overflow-hidden">
        {data.items.length === 0 ? (
          <EmptyState title="The queue is empty" description="Every submitted job has been picked up." />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[720px]">
              <thead className="border-b border-ink-700 bg-ink-900/50">
                <tr><th className="th">Job</th><th className="th">Priority</th><th className="th">Status</th><th className="th">Dispatch</th><th className="th">Waiting since</th></tr>
              </thead>
              <tbody className="divide-y divide-ink-700">
                {data.items.map(({ job, dispatch_status }) => (
                  <tr key={job.job_id}>
                    <td className="td"><Link to={`/jobs/${job.job_id}`} className="font-medium text-ink-100 hover:text-accent-400">{job.name}</Link><div className="font-mono text-xs text-ink-400">{job.job_type}</div></td>
                    <td className="td"><PriorityBadge priority={job.priority} /></td>
                    <td className="td"><StateBadge state={job.state} /></td>
                    <td className={`td text-xs font-medium ${DISPATCH[dispatch_status].className}`}>
                      {DISPATCH[dispatch_status].label}
                      {dispatch_status === 'scheduled_retry' && job.next_attempt_at && <span className="ml-1 text-ink-400">({formatRelative(job.next_attempt_at)})</span>}
                    </td>
                    <td className="td text-ink-300">{formatRelative(job.queued_at ?? job.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      {data.unpublished_events > 0 && (
        <p className="mt-3 text-xs text-amber-300">{data.unpublished_events} dispatch message(s) not yet confirmed by the broker — the relay will keep retrying.</p>
      )}
    </>
  )
}
