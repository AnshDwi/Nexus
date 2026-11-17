import { AlertOctagon, CheckCircle2, Layers, ListChecks, PlayCircle, Server } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { StateBadge } from '../components/Badges'
import { PageHeader } from '../components/PageHeader'
import { StatCard } from '../components/StatCard'
import { EmptyState, ErrorState, LoadingState } from '../components/States'
import { formatRelative } from '../lib/format'
import { useSummary } from '../lib/queries'

export function OverviewPage() {
  const { data, isLoading, error, refetch } = useSummary()
  if (isLoading) return <LoadingState rows={6} label="Loading overview" />
  if (error || !data) return <ErrorState error={error} onRetry={() => void refetch()} />

  const w = data.workers
  const trend = data.trend.map((p) => ({
    ...p,
    label: new Date(p.bucket_start).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' }),
  }))
  const hasTrend = trend.some((p) => p.completed || p.failed)

  return (
    <>
      <PageHeader
        title="Overview"
        description="Live state of jobs, the dispatch queue and the worker fleet."
        actions={<Link to="/jobs?create=1" className="btn-primary">New job</Link>}
      />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-6">
        <StatCard label="Total jobs" value={data.total_jobs} icon={ListChecks} />
        <StatCard label="Running" value={data.running} icon={PlayCircle} tone="blue" />
        <StatCard label="Completed" value={data.completed} icon={CheckCircle2} tone="green" />
        <StatCard label="Failed" value={data.failed} icon={AlertOctagon} tone="red" />
        <StatCard
          label="Queue depth"
          value={data.queue_depth}
          icon={Layers}
          tone="amber"
          hint={data.broker.available ? `${data.broker.ready_messages ?? 0} in broker · ${data.broker.dead_letter_messages ?? 0} dead-lettered` : 'broker unavailable'}
        />
        <StatCard
          label="Worker health"
          value={`${w.healthy}/${w.total}`}
          icon={Server}
          tone={w.total > 0 && w.healthy === w.total ? 'green' : w.healthy === 0 ? 'red' : 'amber'}
          hint={`${w.busy}/${w.capacity} slots busy`}
        />
      </div>

      <div className="mt-6 grid gap-6 xl:grid-cols-3">
        <section className="card p-4 xl:col-span-2" aria-label="Execution trends">
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-ink-100">Execution trends</h2>
            <span className="text-xs text-ink-400">last 24 hours · hourly</span>
          </div>
          {hasTrend ? (
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={trend} margin={{ left: -20, right: 8, top: 4 }}>
                  <defs>
                    <linearGradient id="ok" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#34d399" stopOpacity={0.35} /><stop offset="100%" stopColor="#34d399" stopOpacity={0} /></linearGradient>
                    <linearGradient id="bad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#fb7185" stopOpacity={0.35} /><stop offset="100%" stopColor="#fb7185" stopOpacity={0} /></linearGradient>
                  </defs>
                  <CartesianGrid stroke="#1c2944" vertical={false} />
                  <XAxis dataKey="label" stroke="#64748b" fontSize={11} tickLine={false} axisLine={false} minTickGap={24} />
                  <YAxis stroke="#64748b" fontSize={11} tickLine={false} axisLine={false} allowDecimals={false} />
                  <Tooltip contentStyle={{ background: '#0f172a', border: '1px solid #273657', borderRadius: 8, fontSize: 12 }} />
                  <Area type="monotone" dataKey="completed" stroke="#34d399" fill="url(#ok)" strokeWidth={2} />
                  <Area type="monotone" dataKey="failed" stroke="#fb7185" fill="url(#bad)" strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <EmptyState title="No executions yet" description="Completed and failed attempts will chart here." />
          )}
        </section>

        <section className="card" aria-label="Recent activity">
          <div className="border-b border-ink-700 px-4 py-3">
            <h2 className="text-sm font-semibold text-ink-100">Recent activity</h2>
          </div>
          {data.recent_activity.length === 0 ? (
            <EmptyState title="Nothing has happened yet" description="Submit a job to see it move through the pipeline." />
          ) : (
            <ul className="max-h-[19rem] divide-y divide-ink-700 overflow-y-auto">
              {data.recent_activity.map((a, i) => (
                <li key={i} className="px-4 py-2.5">
                  <div className="flex items-center justify-between gap-2">
                    <Link to={`/jobs/${a.job_id}`} className="truncate text-sm font-medium text-ink-100 hover:text-accent-400">{a.job_name}</Link>
                    <span className="shrink-0 text-xs text-ink-400">{formatRelative(a.occurred_at)}</span>
                  </div>
                  <div className="mt-1 flex items-center gap-1.5">
                    <StateBadge state={a.from_state} /><span className="text-ink-500">→</span><StateBadge state={a.to_state} />
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>

      <section className="card mt-6 p-4" aria-label="Jobs by state">
        <h2 className="mb-3 text-sm font-semibold text-ink-100">Jobs by state</h2>
        <div className="flex flex-wrap gap-3">
          {Object.entries(data.by_state).map(([state, count]) => (
            <Link key={state} to={`/jobs?status=${state}`} className="flex items-center gap-2 rounded-lg border border-ink-700 px-3 py-2 hover:border-ink-500">
              <StateBadge state={state as keyof typeof data.by_state} />
              <span className="tabular-nums text-sm text-ink-200">{count}</span>
            </Link>
          ))}
        </div>
      </section>
    </>
  )
}
