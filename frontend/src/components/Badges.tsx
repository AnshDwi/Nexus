import clsx from 'clsx'
import type { ExecutionStatus, JobPriority, JobState, Worker } from '../lib/types'

const base = 'inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset whitespace-nowrap'

const STATE_STYLES: Record<JobState, string> = {
  pending: 'bg-slate-500/10 text-slate-300 ring-slate-500/30',
  queued: 'bg-sky-500/10 text-sky-300 ring-sky-500/30',
  running: 'bg-blue-500/15 text-blue-300 ring-blue-500/40',
  completed: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
  failed: 'bg-rose-500/10 text-rose-300 ring-rose-500/30',
  retrying: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
  cancelled: 'bg-zinc-500/10 text-zinc-400 ring-zinc-500/30',
}

export function StateBadge({ state }: { state: JobState }) {
  return (
    <span className={clsx(base, STATE_STYLES[state])} data-testid={`state-${state}`}>
      <span
        className={clsx('h-1.5 w-1.5 rounded-full bg-current', state === 'running' && 'animate-pulse')}
        aria-hidden
      />
      {state.charAt(0).toUpperCase() + state.slice(1)}
    </span>
  )
}

const PRIORITY_STYLES: Record<JobPriority, string> = {
  critical: 'bg-rose-500/10 text-rose-300 ring-rose-500/30',
  high: 'bg-orange-500/10 text-orange-300 ring-orange-500/30',
  normal: 'bg-ink-700/60 text-ink-200 ring-ink-500/40',
  low: 'bg-ink-800 text-ink-400 ring-ink-600/50',
}

export function PriorityBadge({ priority }: { priority: JobPriority }) {
  return <span className={clsx(base, PRIORITY_STYLES[priority])}>{priority}</span>
}

const EXEC_STYLES: Record<ExecutionStatus, string> = {
  running: STATE_STYLES.running,
  completed: STATE_STYLES.completed,
  failed: STATE_STYLES.failed,
  timed_out: 'bg-orange-500/10 text-orange-300 ring-orange-500/30',
  lost: 'bg-fuchsia-500/10 text-fuchsia-300 ring-fuchsia-500/30',
  cancelled: STATE_STYLES.cancelled,
}

export function ExecutionBadge({ status }: { status: ExecutionStatus }) {
  return <span className={clsx(base, EXEC_STYLES[status])}>{status.replace('_', ' ')}</span>
}

export function WorkerStatusBadge({ status }: { status: Worker['status'] }) {
  const styles = {
    healthy: 'bg-emerald-500/10 text-emerald-300 ring-emerald-500/30',
    unhealthy: 'bg-rose-500/10 text-rose-300 ring-rose-500/30',
    draining: 'bg-amber-500/10 text-amber-300 ring-amber-500/30',
  }[status]
  return (
    <span className={clsx(base, styles)}>
      <span className="h-1.5 w-1.5 rounded-full bg-current" aria-hidden />
      {status}
    </span>
  )
}
